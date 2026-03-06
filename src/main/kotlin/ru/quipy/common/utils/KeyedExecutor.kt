package ru.quipy.common.utils

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

class KeyedExecutor(
    stripes: Int,
    threadFactory: ThreadFactory? = null,
) {
    private val executors: List<ExecutorService> = List(stripes.coerceAtLeast(1)) { idx ->
        if (threadFactory == null) {
            Executors.newSingleThreadExecutor()
        } else {
            Executors.newSingleThreadExecutor { r ->
                threadFactory.newThread(r).apply { name = "${name}-$idx" }
            }
        }
    }

    fun execute(key: Any, task: Runnable) {
        val index = ((key.hashCode() and Int.MAX_VALUE) % executors.size)
        executors[index].execute(task)
    }
}
