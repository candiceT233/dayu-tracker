#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>

// Define MAX_CONF_STR_LENGTH
#define MAX_CONF_STR_LENGTH 256
#define H5FD_MAX_FILENAME_LEN 128

// Define EasySingleton template
typedef struct {
    pthread_mutex_t lock;
    void *obj;
} EasySingleton;

// Initialize the EasySingleton instance
EasySingleton traceManagerInstance = {.lock = PTHREAD_MUTEX_INITIALIZER, .obj = NULL};

// Define TraceManager class
typedef struct {
    char *entryPattern;
    char *exitPattern;
    char *jsonPattern;
    int rank;
} TraceManager;

// Define TraceLogger class
typedef struct {
    char *functionName;
} TraceLogger;

// Function prototypes
TraceManager *getTraceManagerInstance(int rank);
TraceLogger *createTraceLogger(const char *functionName);
void destroyTraceLogger(TraceLogger *logger);
void logEvent(TraceLogger *logger, const char *phase);

// Function to get TraceManager instance
TraceManager *getTraceManagerInstance(int rank) {
    pthread_mutex_lock(&traceManagerInstance.lock);
    if (!traceManagerInstance.obj) {
        TraceManager *manager = (TraceManager *)malloc(sizeof(TraceManager));
        manager->entryPattern = "[";
        manager->exitPattern = "]";
        manager->jsonPattern = "{%v},";
        manager->rank = rank;
        traceManagerInstance.obj = manager;
    }
    pthread_mutex_unlock(&traceManagerInstance.lock);
    return (TraceManager *)traceManagerInstance.obj;
}

// Function to create TraceLogger instance
TraceLogger *createTraceLogger(const char *functionName) {
    TraceLogger *logger = (TraceLogger *)malloc(sizeof(TraceLogger));
    logger->functionName = strdup(functionName);
    return logger;
}

// Function to destroy TraceLogger instance
void destroyTraceLogger(TraceLogger *logger) {
    free(logger->functionName);
    free(logger);
}

// Function to log event
void logEvent(TraceLogger *logger, const char *phase) {
    TraceManager *manager = (TraceManager *)traceManagerInstance.obj;
    printf("\"name\": \"%s\", \"ph\": \"%s\", \"ts\": %ld, \"pid\": %d, \"tid\": %ld\n",
           logger->functionName, phase, (long)time(NULL), getpid(), (long)pthread_self());
}

// Macro for demangle function
#define demangle(name) (name)

// Macro for TRACE_FUNC
#ifdef debug_mode
#define TRACE_FUNC(...) \
    TraceLogger *traceLogger = createTraceLogger(__func__); \
    logEvent(traceLogger, "B"); \
    destroyTraceLogger(traceLogger);
#else
#define TRACE_FUNC(...)
#endif

int main() {
    // Usage example
    int rank = 0; // Get the rank somehow
    TraceManager *manager = getTraceManagerInstance(rank);

    // Usage example with TRACE_FUNC
    TRACE_FUNC();

    return 0;
}