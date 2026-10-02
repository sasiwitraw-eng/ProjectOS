/*
 * Advanced Multi-Threaded Producer-Consumer Engine
 * Supports: macOS, Ubuntu/Linux (WSL), and Windows (MSYS2/MinGW)
 */
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "raylib.h"

#define MAX_BUFFER       20
#define MIN_BUFFER       5
#define INITIAL_CAPACITY 10
#define MAX_THREADS      10

#define DELAY_STEP_MS    200
#define DELAY_MIN_MS     200
#define DELAY_MAX_MS     3000

#define SEM_EMPTY_NAME   "/projectos_sem_empty"
#define SEM_FULL_NAME    "/projectos_sem_full"

typedef enum { MODE_SEMAPHORE = 0, MODE_CONDVAR = 1 } SyncMode;

typedef struct {
    int   id;
    int   producer_id;
    Color color;
} BufferItem;

/* ------------------------------------------------------------------ */
/* Shared state                                                        */
/* ------------------------------------------------------------------ */

static BufferItem buffer[MAX_BUFFER];
static int  in_idx = 0;
static int  out_idx = 0;
static int  item_count = 0;
static int  buffer_capacity = INITIAL_CAPACITY;
static int  next_item_id = 1;

static long total_produced = 0;
static long total_consumed = 0;
static long prod_blocked_count = 0;
static long cons_blocked_count = 0;
static int  consumed_in_interval = 0;

static atomic_int  active_producers = 2;
static atomic_int  active_consumers = 2;
static atomic_int  prod_delay_ms = 800;
static atomic_int  cons_delay_ms = 1200;
static atomic_bool running = true;

static SyncMode sync_mode = MODE_SEMAPHORE;

static pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  cond_empty   = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  cond_full    = PTHREAD_COND_INITIALIZER;

#ifdef __APPLE__
static sem_t *sem_empty = SEM_FAILED;
static sem_t *sem_full  = SEM_FAILED;
#else
static sem_t sem_empty_obj;
static sem_t sem_full_obj;
static sem_t *sem_empty = &sem_empty_obj;
static sem_t *sem_full  = &sem_full_obj;
#endif

static Color producer_colors[MAX_THREADS];

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static void nap_ms(int ms) {
    while (ms > 0 && atomic_load(&running)) {
        int chunk = ms > 50 ? 50 : ms;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = (long)chunk * 1000000L };
        nanosleep(&ts, NULL);
        ms -= chunk;
    }
}

static void sem_wait_retry(sem_t *s) {
    while (sem_wait(s) != 0 && errno == EINTR) { /* retry */ }
}

/* ------------------------------------------------------------------ */
/* Producer / Consumer critical sections                               */
/* ------------------------------------------------------------------ */

static bool produce_item(int prod_id) {
    if (sync_mode == MODE_SEMAPHORE) {
        if (sem_trywait(sem_empty) != 0) {
            pthread_mutex_lock(&buffer_mutex);
            prod_blocked_count++;
            pthread_mutex_unlock(&buffer_mutex);
            sem_wait_retry(sem_empty);
        }
        if (!atomic_load(&running)) return false;
        pthread_mutex_lock(&buffer_mutex);
    } else {
        pthread_mutex_lock(&buffer_mutex);
        if (item_count >= buffer_capacity) {
            prod_blocked_count++;
            while (item_count >= buffer_capacity && atomic_load(&running))
                pthread_cond_wait(&cond_empty, &buffer_mutex);
        }
        if (!atomic_load(&running)) {
            pthread_mutex_unlock(&buffer_mutex);
            return false;
        }
    }

    assert(item_count < buffer_capacity);
    buffer[in_idx].id          = next_item_id++;
    buffer[in_idx].producer_id = prod_id;
    buffer[in_idx].color       = producer_colors[prod_id];
    in_idx = (in_idx + 1) % buffer_capacity;
    item_count++;
    total_produced++;

    if (sync_mode == MODE_SEMAPHORE) {
        pthread_mutex_unlock(&buffer_mutex);
        sem_post(sem_full);
    } else {
        pthread_cond_signal(&cond_full);
        pthread_mutex_unlock(&buffer_mutex);
    }
    return true;
}

static bool consume_item(void) {
    if (sync_mode == MODE_SEMAPHORE) {
        if (sem_trywait(sem_full) != 0) {
            pthread_mutex_lock(&buffer_mutex);
            cons_blocked_count++;
            pthread_mutex_unlock(&buffer_mutex);
            sem_wait_retry(sem_full);
        }
        if (!atomic_load(&running)) return false;
        pthread_mutex_lock(&buffer_mutex);
    } else {
        pthread_mutex_lock(&buffer_mutex);
        if (item_count == 0) {
            cons_blocked_count++;
            while (item_count == 0 && atomic_load(&running))
                pthread_cond_wait(&cond_full, &buffer_mutex);
        }
        if (!atomic_load(&running)) {
            pthread_mutex_unlock(&buffer_mutex);
            return false;
        }
    }

    assert(item_count > 0);
    buffer[out_idx].id = 0;
    out_idx = (out_idx + 1) % buffer_capacity;
    item_count--;
    total_consumed++;
    consumed_in_interval++;

    if (sync_mode == MODE_SEMAPHORE) {
        pthread_mutex_unlock(&buffer_mutex);
        sem_post(sem_empty);
    } else {
        pthread_cond_signal(&cond_empty);
        pthread_mutex_unlock(&buffer_mutex);
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* Worker threads                                                      */
/* ------------------------------------------------------------------ */

static void *producer_worker(void *arg) {
    int id = (int)(intptr_t)arg;
    while (atomic_load(&running)) {
        if (id < atomic_load(&active_producers)) {
            if (!produce_item(id)) break;
            nap_ms(atomic_load(&prod_delay_ms));
        } else {
            nap_ms(100);
        }
    }
    return NULL;
}

static void *consumer_worker(void *arg) {
    int id = (int)(intptr_t)arg;
    while (atomic_load(&running)) {
        if (id < atomic_load(&active_consumers)) {
            if (!consume_item()) break;
            nap_ms(atomic_load(&cons_delay_ms));
        } else {
            nap_ms(100);
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Change buffer capacity at run time                                  */
/* ------------------------------------------------------------------ */

static bool change_capacity(int delta) {
    bool ok = true;
    pthread_mutex_lock(&buffer_mutex);

    int newcap = buffer_capacity + delta;
    if (newcap >= MIN_BUFFER && newcap <= MAX_BUFFER) {
        if (newcap < item_count) {
            ok = false;
        } else if (delta < 0 && sync_mode == MODE_SEMAPHORE && sem_trywait(sem_empty) != 0) {
            ok = false;
        } else {
            BufferItem tmp[MAX_BUFFER];
            for (int k = 0; k < item_count; k++)
                tmp[k] = buffer[(out_idx + k) % buffer_capacity];
            memset(buffer, 0, sizeof(buffer));
            for (int k = 0; k < item_count; k++)
                buffer[k] = tmp[k];
            out_idx = 0;
            in_idx = item_count % newcap;
            buffer_capacity = newcap;

            if (delta > 0) {
                if (sync_mode == MODE_SEMAPHORE) sem_post(sem_empty);
                else pthread_cond_broadcast(&cond_empty);
            }
        }
    }

    pthread_mutex_unlock(&buffer_mutex);
    return ok;
}

/* ------------------------------------------------------------------ */
/* UI helpers                                                          */
/* ------------------------------------------------------------------ */

static void DrawProgressBar(Rectangle rect, float progress) {
    if (progress < 0.0f) progress = 0.0f;
    if (progress > 1.0f) progress = 1.0f;
    DrawRectangleRec(rect, DARKGRAY);
    DrawRectangle((int)rect.x, (int)rect.y, (int)(rect.width * progress), (int)rect.height, GOLD);
    DrawRectangleLinesEx(rect, 1.0f, GRAY);
}

static bool UiButton(Rectangle bounds, const char *text, Color baseColor) {
    Vector2 mouse = GetMousePosition();
    bool clicked = false;
    Color btn = baseColor;

    if (CheckCollisionPointRec(mouse, bounds)) {
        btn = (Color){ (unsigned char)fminf(baseColor.r + 30, 255),
                       (unsigned char)fminf(baseColor.g + 30, 255),
                       (unsigned char)fminf(baseColor.b + 30, 255), 255 };
        if (IsMouseButtonPressed(MOUSE_BUTTON_LEFT)) clicked = true;
    }

    DrawRectangleRec(bounds, btn);
    DrawRectangleLinesEx(bounds, 2.0f, DARKGRAY);
    DrawText(text,
             (int)(bounds.x + (bounds.width - MeasureText(text, 18)) / 2),
             (int)(bounds.y + (bounds.height - 18) / 2), 18, BLACK);
    return clicked;
}

static int select_mode_screen(void) {
    while (!WindowShouldClose()) {
        int pick = -1;
        BeginDrawing();
        ClearBackground((Color){ 245, 245, 247, 255 });
        DrawText("Producer-Consumer Multi-Threaded Engine", 215, 150, 28, DARKGRAY);
        DrawText("Select synchronization mode", 365, 210, 20, GRAY);
        if (UiButton((Rectangle){ 312, 270, 400, 60 }, "POSIX Semaphore + Mutex", SKYBLUE)) pick = 0;
        if (UiButton((Rectangle){ 312, 350, 400, 60 }, "Mutex + Condition Variable", ORANGE)) pick = 1;
        EndDrawing();
        if (pick >= 0) return pick;
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    int mode_arg = -1;
    if (argc > 1) {
        if (strcmp(argv[1], "sem") == 0)       mode_arg = MODE_SEMAPHORE;
        else if (strcmp(argv[1], "cond") == 0) mode_arg = MODE_CONDVAR;
        else {
            fprintf(stderr, "Usage: %s [sem|cond]\n", argv[0]);
            return 1;
        }
    }

    producer_colors[0] = RED;    producer_colors[1] = ORANGE;
    producer_colors[2] = GOLD;   producer_colors[3] = LIME;
    producer_colors[4] = GREEN;  producer_colors[5] = SKYBLUE;
    producer_colors[6] = BLUE;   producer_colors[7] = PURPLE;
    producer_colors[8] = PINK;   producer_colors[9] = MAROON;

    InitWindow(1024, 680, "Advanced Multi-Threaded Producer-Consumer Engine");
    SetTargetFPS(60);

    if (mode_arg < 0) {
        mode_arg = select_mode_screen();
        if (mode_arg < 0) { CloseWindow(); return 0; }
    }
    sync_mode = (SyncMode)mode_arg;

    if (sync_mode == MODE_SEMAPHORE) {
#ifdef __APPLE__
        sem_unlink(SEM_EMPTY_NAME);
        sem_unlink(SEM_FULL_NAME);
        sem_empty = sem_open(SEM_EMPTY_NAME, O_CREAT | O_EXCL, 0600, (unsigned)INITIAL_CAPACITY);
        sem_full  = sem_open(SEM_FULL_NAME,  O_CREAT | O_EXCL, 0600, 0u);
        if (sem_empty == SEM_FAILED || sem_full == SEM_FAILED) {
            perror("sem_open");
            CloseWindow();
            return 1;
        }
#else
        if (sem_init(sem_empty, 0, (unsigned)INITIAL_CAPACITY) != 0 ||
            sem_init(sem_full,  0, 0u) != 0) {
            perror("sem_init");
            CloseWindow();
            return 1;
        }
#endif
    }

    pthread_t prod_threads[MAX_THREADS];
    pthread_t cons_threads[MAX_THREADS];
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_create(&prod_threads[i], NULL, producer_worker, (void *)(intptr_t)i);
        pthread_create(&cons_threads[i], NULL, consumer_worker, (void *)(intptr_t)i);
    }

    const char *mode_label = (sync_mode == MODE_SEMAPHORE) ? "Mode: POSIX Semaphore" : "Mode: Mutex + CondVar";
    Color mode_col = (sync_mode == MODE_SEMAPHORE) ? SKYBLUE : ORANGE;

    float  throughput = 0.0f;
    double last_time_check = GetTime();
    double shrink_warn_until = 0.0;

    while (!WindowShouldClose()) {
        double now = GetTime();

        if (now - last_time_check >= 1.0) {
            pthread_mutex_lock(&buffer_mutex);
            int n = consumed_in_interval;
            consumed_in_interval = 0;
            pthread_mutex_unlock(&buffer_mutex);
            throughput = (float)n / (float)(now - last_time_check);
            last_time_check = now;
        }

        BufferItem snap[MAX_BUFFER];
        int  cap, cnt;
        long p_blocked, c_blocked, t_prod, t_cons;
        pthread_mutex_lock(&buffer_mutex);
        memcpy(snap, buffer, sizeof(snap));
        cap = buffer_capacity;
        cnt = item_count;
        p_blocked = prod_blocked_count;
        c_blocked = cons_blocked_count;
        t_prod = total_produced;
        t_cons = total_consumed;
        pthread_mutex_unlock(&buffer_mutex);

        int n_prod = atomic_load(&active_producers);
        int n_cons = atomic_load(&active_consumers);
        int pd = atomic_load(&prod_delay_ms);
        int cd = atomic_load(&cons_delay_ms);

        BeginDrawing();
        ClearBackground((Color){ 245, 245, 247, 255 });

        DrawRectangle(0, 0, 1024, 60, (Color){ 30, 41, 59, 255 });
        DrawText("Producer-Consumer Multi-Threaded Engine", 20, 18, 22, WHITE);
        DrawRectangleRounded((Rectangle){ 750, 12, 250, 36 }, 0.3f, 4, mode_col);
        DrawText(mode_label, 750 + (250 - MeasureText(mode_label, 18)) / 2, 21, 18, BLACK);

        DrawRectangleRounded((Rectangle){ 20, 80, 640, 360 }, 0.03f, 4, WHITE);
        DrawRectangleLinesEx((Rectangle){ 20, 80, 640, 360 }, 2.0f, LIGHTGRAY);
        DrawText("Bounded Buffer Queue", 40, 95, 20, DARKGRAY);

        const int cols = 5;
        for (int i = 0; i < cap; i++) {
            int row = i / cols;
            int col = i % cols;
            int x = 45 + col * 115;
            int y = 128 + row * 77;

            DrawRectangleRounded((Rectangle){ (float)x, (float)y, 100, 65 }, 0.1f, 4, (Color){ 240, 240, 242, 255 });
            DrawRectangleLinesEx((Rectangle){ (float)x, (float)y, 100, 65 }, 1.5f, GRAY);
            DrawText(TextFormat("Slot %d", i), x + 8, y + 6, 12, DARKGRAY);

            if (snap[i].id != 0) {
                DrawRectangleRounded((Rectangle){ (float)(x + 10), (float)(y + 24), 80, 32 }, 0.2f, 4, snap[i].color);
                DrawText(TextFormat("#%d", snap[i].id), x + 25, y + 30, 18, WHITE);
            }
        }

        DrawRectangleRounded((Rectangle){ 680, 80, 324, 360 }, 0.03f, 4, WHITE);
        DrawRectangleLinesEx((Rectangle){ 680, 80, 324, 360 }, 2.0f, LIGHTGRAY);
        DrawText("Control Panel", 700, 95, 20, DARKGRAY);

        DrawText(TextFormat("Producers (N): %d", n_prod), 700, 135, 16, BLACK);
        if (UiButton((Rectangle){ 880, 130, 35, 28 }, "-", LIGHTGRAY) && n_prod > 1)
            atomic_fetch_sub(&active_producers, 1);
        if (UiButton((Rectangle){ 925, 130, 35, 28 }, "+", LIGHTGRAY) && n_prod < MAX_THREADS)
            atomic_fetch_add(&active_producers, 1);

        DrawText(TextFormat("Consumers (M): %d", n_cons), 700, 175, 16, BLACK);
        if (UiButton((Rectangle){ 880, 170, 35, 28 }, "-", LIGHTGRAY) && n_cons > 1)
            atomic_fetch_sub(&active_consumers, 1);
        if (UiButton((Rectangle){ 925, 170, 35, 28 }, "+", LIGHTGRAY) && n_cons < MAX_THREADS)
            atomic_fetch_add(&active_consumers, 1);

        DrawText(TextFormat("Capacity: %d", cap), 700, 215, 16, BLACK);
        if (UiButton((Rectangle){ 880, 210, 35, 28 }, "-", LIGHTGRAY) && cap > MIN_BUFFER) {
            if (!change_capacity(-1)) shrink_warn_until = now + 1.5;
        }
        if (UiButton((Rectangle){ 925, 210, 35, 28 }, "+", LIGHTGRAY) && cap < MAX_BUFFER)
            change_capacity(+1);
        if (now < shrink_warn_until)
            DrawText("Buffer too full to shrink", 700, 238, 10, RED);

        DrawText(TextFormat("Prod Delay: %.1fs", pd / 1000.0f), 700, 255, 16, BLACK);
        if (UiButton((Rectangle){ 880, 250, 35, 28 }, "-", LIGHTGRAY) && pd > DELAY_MIN_MS)
            atomic_fetch_sub(&prod_delay_ms, DELAY_STEP_MS);
        if (UiButton((Rectangle){ 925, 250, 35, 28 }, "+", LIGHTGRAY) && pd < DELAY_MAX_MS)
            atomic_fetch_add(&prod_delay_ms, DELAY_STEP_MS);

        DrawText(TextFormat("Cons Delay: %.1fs", cd / 1000.0f), 700, 295, 16, BLACK);
        if (UiButton((Rectangle){ 880, 290, 35, 28 }, "-", LIGHTGRAY) && cd > DELAY_MIN_MS)
            atomic_fetch_sub(&cons_delay_ms, DELAY_STEP_MS);
        if (UiButton((Rectangle){ 925, 290, 35, 28 }, "+", LIGHTGRAY) && cd < DELAY_MAX_MS)
            atomic_fetch_add(&cons_delay_ms, DELAY_STEP_MS);

        DrawText("Active Producer Threads:", 700, 335, 14, DARKGRAY);
        for (int i = 0; i < n_prod; i++) {
            DrawRectangle(700 + (i % 5) * 45, 360 + (i / 5) * 20, 35, 15, producer_colors[i]);
            DrawText(TextFormat("P%d", i), 700 + (i % 5) * 45 + 8, 360 + (i / 5) * 20 + 1, 12, WHITE);
        }

        DrawRectangleRounded((Rectangle){ 20, 460, 984, 200 }, 0.02f, 4, (Color){ 30, 41, 59, 255 });
        DrawText("Real-Time Analytics & System Status", 40, 475, 20, SKYBLUE);

        float utilization = ((float)cnt / (float)cap) * 100.0f;

        DrawRectangleRounded((Rectangle){ 40, 510, 210, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("THROUGHPUT", 55, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("%.1f", throughput), 55, 550, 32, GREEN);
        DrawText("items / sec", 55, 595, 14, LIGHTGRAY);

        DrawRectangleRounded((Rectangle){ 270, 510, 210, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("UTILIZATION", 285, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("%.1f%%", utilization), 285, 550, 32, GOLD);
        DrawProgressBar((Rectangle){ 285, 595, 180, 15 }, utilization / 100.0f);
        DrawText(TextFormat("%d / %d slots", cnt, cap), 285, 615, 12, LIGHTGRAY);

        DrawRectangleRounded((Rectangle){ 500, 510, 220, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("THREAD BLOCKS", 515, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("Prod: %ld", p_blocked), 515, 555, 18, RED);
        DrawText(TextFormat("Cons: %ld", c_blocked), 515, 585, 18, ORANGE);

        DrawRectangleRounded((Rectangle){ 740, 510, 240, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("TOTAL PROCESSED", 755, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("Produced: %ld", t_prod), 755, 555, 18, WHITE);
        DrawText(TextFormat("Consumed: %ld", t_cons), 755, 585, 18, WHITE);

        EndDrawing();
    }

    atomic_store(&running, false);
    if (sync_mode == MODE_SEMAPHORE) {
        for (int i = 0; i < MAX_THREADS; i++) {
            sem_post(sem_empty);
            sem_post(sem_full);
        }
    }
    pthread_mutex_lock(&buffer_mutex);
    pthread_cond_broadcast(&cond_empty);
    pthread_cond_broadcast(&cond_full);
    pthread_mutex_unlock(&buffer_mutex);

    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(prod_threads[i], NULL);
        pthread_join(cons_threads[i], NULL);
    }

    CloseWindow();

    if (sync_mode == MODE_SEMAPHORE) {
#ifdef __APPLE__
        sem_close(sem_empty);
        sem_close(sem_full);
        sem_unlink(SEM_EMPTY_NAME);
        sem_unlink(SEM_FULL_NAME);
#else
        sem_destroy(sem_empty);
        sem_destroy(sem_full);
#endif
    }
    pthread_mutex_destroy(&buffer_mutex);
    pthread_cond_destroy(&cond_empty);
    pthread_cond_destroy(&cond_full);
    return 0;
}