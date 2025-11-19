package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        // Собираем все futures от адаптеров
        val futures: List<CompletableFuture<Boolean>> = paymentAccounts.map { account ->
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }

        // CompletableFuture, который завершится когда все futures завершены
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            // После allOf — все futures завершены, можно безопасно join() и агрегировать (AND)
            futures.map { it.join() }.all { it }
        }
    }
}