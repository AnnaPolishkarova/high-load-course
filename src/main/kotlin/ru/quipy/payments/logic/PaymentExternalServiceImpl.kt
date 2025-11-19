package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.prometheus.metrics.core.metrics.Summary
import io.prometheus.metrics.model.registry.PrometheusRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.time.DurationUnit
import kotlin.time.measureTime
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.max

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val timeOut = Duration.ofSeconds(0)
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val dispatcherExecutor = Executors.newFixedThreadPool(max(200, parallelRequests / 2))

    private val dispatcher = Dispatcher(dispatcherExecutor).apply {
        maxRequests = parallelRequests
        maxRequestsPerHost = parallelRequests
    }

    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .connectionPool(ConnectionPool(20000, 20, TimeUnit.SECONDS))
        .readTimeout(Duration.ofSeconds(30))
        .build()

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = OngoingWindow(parallelRequests)

    // Объявление счетчиков метрик
    private val paymentAttemptsTotal: Counter = Counter.builder("payment_attempts_total")
        .description("Payment attempts sent to provider")
//            .tag("account", accountName)
        .register(meterRegistry)

    private val paymentSuccessTotal: Counter = Counter.builder("payment_success_total")
        .description("Successfully processed payments")
//            .tag("account", accountName)
        .register(meterRegistry)

    private val paymentFailureTotal: Counter = Counter.builder("payment_failure_total")
        .description("Failed payments")
//            .tag("account", accountName)
        .register(meterRegistry)

    private val paymentCompletedTotal: Counter = Counter.builder("payment_completed_total")
        .description("Payments completed total")
//            .tag("account", accountName)
        .register(meterRegistry)

    // Метрика для тайм-аутов
    private val paymentTimeoutCounter: Counter = Counter.builder("payment_timeout_total")
        .description("Total payment timeout")
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentAttemptsTotal.increment()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")


        val cf = CompletableFuture<Boolean>()

        try {
            ongoingWindow.acquire()
            slidingWindowRateLimiter.tickBlocking()

            var urlString = if (timeOut != Duration.ofSeconds(0)) {
                "http://$paymentProviderHostPort/external/process?timeout=$timeOut&serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            } else {
                "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            }

            val request = Request.Builder().run {
                url(urlString)
                post(emptyBody)
            }.build()



            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    try {
                        paymentFailureTotal.increment()
                        if (e is SocketTimeoutException) {
                            paymentTimeoutCounter.increment()
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on timeout for payment $paymentId, txId: $transactionId", u)
                            }
                        } else {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on failure for payment $paymentId, txId: $transactionId", u)
                            }
                        }
                    } finally {
                        try { ongoingWindow.release() } catch (u: Exception) { logger.error("[$accountName] Error releasing ongoingWindow", u) }
                        paymentCompletedTotal.increment()
                        cf.complete(false)
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bodyText = try {
                            response.body?.string()
                        } catch (e: Exception) {
                            null
                        }

                        val body = try {
                            mapper.readValue(bodyText, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            paymentFailureTotal.increment()
                            logger.error(
                                "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                        "payment: $paymentId, result code: ${response.code}, reason: ${bodyText}",
                                e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message ?: bodyText)
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )

                        val result = body.result

                        if (result) {
                            paymentSuccessTotal.increment()
                        } else {
                            paymentFailureTotal.increment()
                        }

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES on response for payment $paymentId, txId: $transactionId", u)
                        }

                        cf.complete(result)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        try {
                            paymentFailureTotal.increment()
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES in exception handler for payment $paymentId, txId: $transactionId", u)
                        } finally {
                            cf.complete(false)
                        }
                    } finally {
                        try { ongoingWindow.release() } catch (u: Exception) { logger.error("[$accountName] Error releasing ongoingWindow", u) }
                        paymentCompletedTotal.increment()
                    }
                }
            })
        } catch (e: Exception) {
            paymentFailureTotal.increment()
            when (e) {
                is SocketTimeoutException -> {
                    paymentTimeoutCounter.increment()
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            ongoingWindow.release()
        } finally {
            paymentCompletedTotal.increment()
        }

        return cf
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()