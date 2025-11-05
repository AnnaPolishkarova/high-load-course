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
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
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

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(64_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val bucketQueue = LeakingBucketRateLimiter(rate = 10, window = Duration.ofSeconds(1), bucketSize = 10)

    // Метрика для подсчета повторных вызовов//////////////
    private val paymentRetryCounter: Counter by lazy {
        Counter.builder("payment_retry_attempts_total")
        .description("Total payment retry attempts")
        .register(meterRegistry)}

    // Метрика для анализа возможностей retry/////////////////
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
        var result = false

        if (!bucketQueue.tick()) {
            return null
        }

        paymentExecutor.submit {

            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            var attempt = 0 ////////////

            while (!result && (deadline - System.currentTimeMillis()) >= 0) {

                attempt++ ////////
                val attemptStartTime = System.currentTimeMillis()/////

                var paymentTime = measureTime {
                    result = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                }

                requestLatency.record(paymentTime.toLong(DurationUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

                val timeLeft = deadline - attemptStartTime//////////
                logger.info("Payment $paymentId attempt #$attempt: result=$result, timeLeft=${timeLeft}ms")///////

                if (!result) {////////////
                    if (timeLeft > 2000) {
                        paymentRetryOpportunityCounter.increment()
//                        logger.info("Retry opportunity: Payment $paymentId could be retried, $timeLeft ms left")
                    }
                    paymentRetryCounter.increment()
                }
            }
//            logger.info("Payment $paymentId final: result=$result, totalAttempts=$attempt")
            }
        return createdAt
    }
}