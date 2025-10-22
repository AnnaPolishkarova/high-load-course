package ru.quipy.payments.logic

//import io.prometheus.metrics.core.metrics.Gauge
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.CompositeRateLimiter
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
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter( /////////////
        rate = 11,
        window = Duration.ofSeconds(1))

    private val bucketQueue = LeakingBucketRateLimiter(rate = 3, window = Duration.ofSeconds(1), bucketSize = 90 * 3)

    private val compositeRateLimiter = CompositeRateLimiter(slidingWindowRateLimiter, bucketQueue)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {


//        if (!slidingWindowRateLimiter.tick()) { /////////////
//            return null
//        }
//
//        if (!bucketQueue.tick()) { /////////////
//            return null
//        }

        if (!compositeRateLimiter.tick()) { /////////////
            return null
        }

        val createdAt = System.currentTimeMillis()


        paymentExecutor.submit {


            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline) ////////////
        }
        return createdAt
    }
}