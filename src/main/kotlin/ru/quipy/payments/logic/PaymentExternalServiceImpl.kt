package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.KeyedExecutor
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import okhttp3.Call
import okhttp3.Callback
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.Protocol
import okhttp3.Response
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val paymentsEsExecutor: KeyedExecutor,
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

    private val clients: List<OkHttpClient> = List(15) { idx ->
        val exec = Executors.newFixedThreadPool(max(200, parallelRequests / 20))
        val dispatcher = Dispatcher(exec).apply {
            maxRequests = max(200, parallelRequests / 20)
            maxRequestsPerHost = max(200, parallelRequests / 20)
        }

        OkHttpClient.Builder()
            .dispatcher(dispatcher)
            .connectionPool(ConnectionPool(100, 10, TimeUnit.SECONDS))
            .readTimeout(Duration.ofSeconds(30))
            .retryOnConnectionFailure(true)
            .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .build()
    }

    private val clientIndex = AtomicInteger(0)

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of( //////////////
        accountName,
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(5)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .waitDurationInOpenState(Duration.ofMillis(1000))
            .minimumNumberOfCalls(10)
            .permittedNumberOfCallsInHalfOpenState(5)
            .recordExceptions(IOException::class.java, SocketTimeoutException::class.java)
            .build()
    )


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

    private val latencySamplesLock = Any()
    private val latencySamplesMs: MutableList<Long> = ArrayList(500)
    private val latencyMs = AtomicLong(-1L)

    private val hedgedScheduler = Executors.newScheduledThreadPool(4, NamedThreadFactory("hedged-scheduler"))

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        val cf = CompletableFuture<Boolean>()
        val finalized = AtomicBoolean(false)

        fun recordLatencyAndMaybeInitP(durationMs: Long) {
            if (latencyMs.get() > 0) return
            val percentil = 0.1
            var computedP: Long? = null
            synchronized(latencySamplesLock) {
                latencySamplesMs.add(durationMs)
                if (latencySamplesMs.size > 500) {
                    val sorted = latencySamplesMs.sorted()
                    val pIndex = ((sorted.size * percentil).toInt()).coerceIn(0, sorted.size - 1)
                    computedP = sorted[pIndex]
                }
            }

            if (computedP != null) {
                latencyMs.compareAndSet(-1L, computedP!!)
            }
        }

        fun finalizePayment(result: Boolean) {
            if (finalized.compareAndSet(false, true)) {
                try {
                    cf.complete(result)
                } finally {
                    try {
                        ongoingWindow.releaseWindow()
                    } catch (u: Exception) {
                        logger.error("[$accountName] Error releasing ongoingWindow", u)
                    }
                    paymentCompletedTotal.increment()
                }
            } else {
                // Already finalized, just complete the future if not already (but it should be)
                cf.complete(result) // will return false if already done, no harm
            }
        }

        if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {

            logger.debug("[$accountName] No free slot for payment $paymentId, rejecting")
            cf.complete(false)
            return cf
        }

        fun sendAttempt(attempt: Int) {

            if (!circuitBreaker.tryAcquirePerission()){ ////////////
                logger.debug("[$accountName] Circuit OPEN, skipping attempt $attempt for payment $paymentId")
                finalizePayment(false)
                return
            }

            val transactionId = UUID.randomUUID()

            paymentAttemptsTotal.increment()

            logger.debug("[$accountName] Submitting payment $paymentId, txId: $transactionId")

            paymentsEsExecutor.execute(paymentId, Runnable {
                try {
                    // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                    // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            now(),
                            Duration.ofMillis(now() - paymentStartedAt)
                        )
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error logging submission for $paymentId", e)
                }
            })

            val startedAtNs = System.nanoTime()

            try {

                if (!slidingWindowRateLimiter.tick()) {
                    finalizePayment(false)
                    return
                }

                val urlString = if (timeOut != Duration.ofSeconds(0)) {
                    "http://$paymentProviderHostPort/external/process?timeout=$timeOut&serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                } else {
                    "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                }

                val request = Request.Builder().run {
                    url(urlString)
                    post(emptyBody)
                }.build()

                val idx = clientIndex.getAndIncrement()
                val client = clients[(idx and Int.MAX_VALUE) % clients.size]

                client.newCall(request).enqueue(object : Callback {
                    private fun durationMs(): Long = (System.nanoTime() - startedAtNs) / 1_000_000L

                    override fun onFailure(call: Call, e: IOException) {
                        val d = durationMs()
                        circuitBreaker.onError(d, TimeUnit.MILLISECONDS, e) ////////////////
                        recordLatencyAndMaybeInitP(d)

                        if (e is SocketTimeoutException) {
                            paymentTimeoutCounter.increment()
                        }

                        paymentFailureTotal.increment()

                        when (e) {
                            is SocketTimeoutException -> logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            else -> logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )
                        }

                        paymentsEsExecutor.execute(paymentId, Runnable {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(
                                        false,
                                        now(),
                                        transactionId,
                                        reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                                    )
                                }
                            } catch (u: Exception) {
                                logger.error(
                                    "[$accountName] Error while updating ES on failure for payment $paymentId, txId: $transactionId",
                                    u
                                )
                            }
                        })

                        finalizePayment(false)
                    }

                    override fun onResponse(call: Call, response: Response) {
                        val d = durationMs()

                        if (body.result) { //////////////
                            circuitBreaker.onSuccess(d, TimeUnit.MILLISECONDS)
                        } else {
                            circuitBreaker.onError(d, TimeUnit.MILLISECONDS, RuntimeException("Payment return false"))
                        }

                        recordLatencyAndMaybeInitP(d)

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

                        logger.debug(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )

                        val result = body.result
                        if (result) {
                            paymentSuccessTotal.increment()
                        } else {
                            paymentFailureTotal.increment()
                        }

                        paymentsEsExecutor.execute(paymentId, Runnable {
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                                }
                            } catch (u: Exception) {
                                logger.error(
                                    "[$accountName] Error while updating ES on response for payment $paymentId, txId: $transactionId",
                                    u
                                )
                            }
                        })

                        finalizePayment(result)
                    }
                })
            } catch (e: Exception) {
                val d = (System.nanoTime() - startedAtNs) / 1_000_000L
                recordLatencyAndMaybeInitP(d)

                paymentFailureTotal.increment()
                when (e) {
                    is SocketTimeoutException -> {
                        paymentTimeoutCounter.increment()
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }

                paymentsEsExecutor.execute(paymentId, Runnable {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    } catch (u: Exception) {
                        logger.error("[$accountName] Error updating ES in outer catch for $paymentId", u)
                    }
                })

                finalizePayment(false)
            }
        }

        sendAttempt(1)

        // Максимальное количество попыток переотправки, по умолчанию 1.
        val maxAttempts = 10
        val baseDelay = latencyMs.get()
        for (attempt in 2..maxAttempts) {
            val delay = baseDelay * (attempt - 1)
            val now = System.currentTimeMillis()
            if (delay >= deadline - now) {
                continue
            }
            hedgedScheduler.schedule({
                if (!cf.isDone) {
                    sendAttempt(attempt)
                }
            }, delay, TimeUnit.MILLISECONDS)
        }

        return cf
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()