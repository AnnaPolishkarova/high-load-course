package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.CountingErrorMeter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.time.DurationUnit
import kotlin.time.measureTime

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        5000,  // corePoolSize
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            setMaximumPoolSize(5000)
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            setRejectedExecutionHandler(CallerBlockingRejectedExecutionHandler())
            // Позволяет удалять отменённые задачи из очереди
            setRemoveOnCancelPolicy(true)
        }
    }

    private val bucketQueue = LeakingBucketRateLimiter(rate = 2000, window = Duration.ofMillis(1000), bucketSize = 8000)

    // Метрика для подсчета повторных вызовов
    private val paymentRetryCounter: Counter by lazy {
        Counter.builder("payment_retry_attempts_total")
        .description("Total payment retry attempts")
        .register(meterRegistry)}

    // Метрика для анализа возможностей retry
    private val paymentRetryOpportunityCounter: Counter by lazy {
        Counter.builder("payment_retry_opportunity_total")
            .description("Payments that could be retried (timeout with time remaining)")
            .register(meterRegistry)
    }

    private val requestLatency: Timer by lazy {
        Timer.builder("request_latency")
            .description("Request latency.")
            .publishPercentiles(0.5, 0.8, 0.9)
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        if (!bucketQueue.tick()) {
            return null
        }

        paymentExecutor.submit {

            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now
        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val future = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)

        val start = System.currentTimeMillis()

        future
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->

                val elapsed = System.currentTimeMillis() - start
                requestLatency.record(elapsed, TimeUnit.MILLISECONDS)

                when {
                    error != null -> {
                        // Timeout OR exception
                        paymentRetryCounter.increment()

                        if (timeLeft > 2000) {
                            paymentRetryOpportunityCounter.increment()
                        }

                        logger.warn(
                            "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                    "timeLeft=${deadline - System.currentTimeMillis()}ms"
                        )

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }

                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded")
                    }

                    success == false -> {
                        paymentRetryCounter.increment()

                        if (timeLeft > 2000) {
                            paymentRetryOpportunityCounter.increment()
                        }

                        logger.info("Payment $paymentId attempt #$attempt returned failure")

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, paymentExecutor)  // callback тоже идёт в ваш executor и НЕ блокирует его
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now
        if (timeLeft <= 0) return

        val baseBackoff = (100L shl (attempt - 1)).coerceAtMost(2000L)
        val jitter = ThreadLocalRandom.current().nextLong(0, 100L)
        val delayMs = minOf(baseBackoff + jitter, timeLeft)

        paymentExecutor.schedule(
            {
                retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
            },
            delayMs,
            TimeUnit.MILLISECONDS
        )
    }


}