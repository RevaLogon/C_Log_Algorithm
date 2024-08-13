#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define FILE_PATH "/home/kuzu/Desktop/output3.txt"
#define NUMBER_OF_WRITES 100000
#define NUMBER_OF_THREADS 10
#define BUFFER_SIZE 1048576  // 1 MB buffer size

// Node structure for the queue
typedef struct Node {
    char *data;
    struct Node *next;
} Node;

// ConcurrentQueue structure
typedef struct {
    Node *front;
    Node *rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} ConcurrentQueue;

void initQueue(ConcurrentQueue *queue) {
    queue->front = queue->rear = NULL;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

void enqueue(ConcurrentQueue *queue, const char *data) {
    Node *node = (Node *)malloc(sizeof(Node));
    node->data = strdup(data);
    node->next = NULL;

    pthread_mutex_lock(&queue->mutex);

    if (queue->rear == NULL) {
        queue->front = queue->rear = node;
    } else {
        queue->rear->next = node;
        queue->rear = node;
    }

    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

char *dequeue(ConcurrentQueue *queue) {
    pthread_mutex_lock(&queue->mutex);

    while (queue->front == NULL) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }

    Node *node = queue->front;
    char *data = node->data;
    queue->front = node->next;

    if (queue->front == NULL) {
        queue->rear = NULL;
    }

    pthread_mutex_unlock(&queue->mutex);
    free(node);

    return data;
}

void destroyQueue(ConcurrentQueue *queue) {
    pthread_mutex_lock(&queue->mutex);

    Node *node = queue->front;
    while (node != NULL) {
        Node *temp = node;
        node = node->next;
        free(temp->data);
        free(temp);
    }

    pthread_mutex_unlock(&queue->mutex);
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}

volatile int isWritingComplete = 0;
ConcurrentQueue queue;

typedef struct {
    int threadIndex;
} ThreadData;

void *worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;

    for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        char logMessage[256];
        snprintf(logMessage, sizeof(logMessage), "Thread %d :: %ld - Write %d - : %ld\n", data->threadIndex, (long)getpid(), i + 1, time(NULL));

        enqueue(&queue, logMessage);
    }

    printf("Thread %d completed its work.\n", data->threadIndex);
    free(data);  
    return NULL;
}

void *fileWriter(void *arg) {
    (void)arg;

    int fd = open(FILE_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0644);  
    if (fd < 0) {
        perror("open");
        return NULL;
    }

    char buffer[BUFFER_SIZE];
    size_t bufferPos = 0;

    while (1) {
        char *logMessage = dequeue(&queue);
        if (logMessage) {
            size_t len = strlen(logMessage);
            if (bufferPos + len >= BUFFER_SIZE) {
         
                if (write(fd, buffer, bufferPos) < 0) {
                    perror("write");
                }
                bufferPos = 0;
            }
        
            memcpy(buffer + bufferPos, logMessage, len);
            bufferPos += len;
            free(logMessage);
        }

        pthread_mutex_lock(&queue.mutex);
        if (isWritingComplete && queue.front == NULL) {
            pthread_mutex_unlock(&queue.mutex);
            break;
        }
        pthread_mutex_unlock(&queue.mutex);
    }

    if (bufferPos > 0) {

        if (write(fd, buffer, bufferPos) < 0) {
            perror("write");
        }
    }

    close(fd);
    return NULL;
}

int main() {
    pthread_t threads[NUMBER_OF_THREADS];
    pthread_t fileWriterThread;

    initQueue(&queue);

    struct timeval start, end;

    gettimeofday(&start, NULL);

    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
        ThreadData *data = (ThreadData *)malloc(sizeof(ThreadData));
        data->threadIndex = i;
        if (pthread_create(&threads[i], NULL, worker, (void *)data) != 0) {
            perror("pthread_create");
            return 1;
        }
    }

    if (pthread_create(&fileWriterThread, NULL, fileWriter, NULL) != 0) {
        perror("pthread_create");
        return 1;
    }

    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            perror("pthread_join");
            return 1;
        }
    }

    pthread_mutex_lock(&queue.mutex);
    isWritingComplete = 1;
    pthread_cond_signal(&queue.cond);
    pthread_mutex_unlock(&queue.mutex);

    if (pthread_join(fileWriterThread, NULL) != 0) {
        perror("pthread_join");
        return 1;
    }

    gettimeofday(&end, NULL);

    double elapsed = (end.tv_sec - start.tv_sec) * 1000.0;
    elapsed += (end.tv_usec - start.tv_usec) / 1000.0;

    printf("Total execution time: %.2f ms\n", elapsed);
    printf("All threads have finished executing. Timestamp written to file.\n");

    // Clean up
    destroyQueue(&queue);
    return 0;
}
