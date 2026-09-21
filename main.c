#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <semaphore.h>
#include <fcntl.h>
#include <math.h>
#include "raylib.h"

#define MAX_BUFFER 20
#define MAX_THREADS 10

// โครงสร้างข้อมูลชิ้นงานใน Buffer
typedef struct {
    int id;
    int producer_id;
    Color color;
} BufferItem;

// Global Shared Memory & State
BufferItem buffer[MAX_BUFFER];
int in_idx = 0;
int out_idx = 0;
int item_count = 0;
int buffer_capacity = 10; // 5 ถึง 20

int active_producers = 2; // 1 ถึง 10
int active_consumers = 2; // 1 ถึง 10

float prod_delay = 0.8f; // วินาที
float cons_delay = 1.2f; // วินาที

// Analytics & Dashboard Metrics
long total_produced = 0;
long total_consumed = 0;
long prod_blocked_count = 0;
long cons_blocked_count = 0;
float throughput = 0.0f;
int consumed_in_interval = 0;
double last_time_check = 0.0;

// Sync Mode: 0 = POSIX Semaphore, 1 = Mutex + Condition Variable
int sync_mode = 0; 

// Synchronization Primitives
pthread_mutex_t buffer_mutex;
sem_t *sem_empty;
sem_t *sem_full;

pthread_cond_t cond_empty;
pthread_cond_t cond_full;

// Palette สีประจำตัวสำหรับ Producer Threads (10 Threads)
Color producer_colors[MAX_THREADS] = {
    RED, ORANGE, GOLD, LIME, GREEN, SKYBLUE, BLUE, PURPLE, PINK, MAROON
};

// --- Function Prototypes (ประกาศหัวฟังก์ชันไว้ก่อนป้องกัน C99 Implicit Declaration Error) ---
void DrawProgressBar(Rectangle rect, float progress);
bool GuiButton(Rectangle bounds, const char *text, Color baseColor);

// --- Helper UI Functions ---
void DrawProgressBar(Rectangle rect, float progress) {
    if (progress < 0.0f) progress = 0.0f;
    if (progress > 1.0f) progress = 1.0f;
    DrawRectangleRec(rect, DARKGRAY);
    DrawRectangle(rect.x, rect.y, rect.width * progress, rect.height, GOLD);
    DrawRectangleLinesEx(rect, 1.0f, GRAY);
}

bool GuiButton(Rectangle bounds, const char *text, Color baseColor) {
    Vector2 mousePoint = GetMousePosition();
    bool clicked = false;
    Color btnColor = baseColor;

    if (CheckCollisionPointRec(mousePoint, bounds)) {
        btnColor = (Color){ (unsigned char)fminf(baseColor.r + 30, 255), 
                            (unsigned char)fminf(baseColor.g + 30, 255), 
                            (unsigned char)fminf(baseColor.b + 30, 255), 255 };
        if (IsMouseButtonPressed(MOUSE_BUTTON_LEFT)) clicked = true;
    }

    DrawRectangleRec(bounds, btnColor);
    DrawRectangleLinesEx(bounds, 2.0f, DARKGRAY);
    DrawText(text, bounds.x + (bounds.width - MeasureText(text, 18)) / 2, bounds.y + (bounds.height - 18) / 2, 18, BLACK);
    return clicked;
}

// --- Sync Control Logic ---
void produce_item_sync(int prod_id, BufferItem item) {
    if (sync_mode == 0) { // POSIX Semaphore
        if (item_count >= buffer_capacity) {
            pthread_mutex_lock(&buffer_mutex);
            prod_blocked_count++;
            pthread_mutex_unlock(&buffer_mutex);
        }
        sem_wait(sem_empty);
        pthread_mutex_lock(&buffer_mutex);
    } else { // Mutex + Condition Variable
        pthread_mutex_lock(&buffer_mutex);
        while (item_count >= buffer_capacity) {
            prod_blocked_count++;
            pthread_cond_wait(&cond_empty, &buffer_mutex);
        }
    }

    // Critical Section
    buffer[in_idx] = item;
    in_idx = (in_idx + 1) % buffer_capacity;
    item_count++;
    total_produced++;

    if (sync_mode == 0) {
        pthread_mutex_unlock(&buffer_mutex);
        sem_post(sem_full);
    } else {
        pthread_cond_signal(&cond_full);
        pthread_mutex_unlock(&buffer_mutex);
    }
}

BufferItem consume_item_sync(int cons_id) {
    BufferItem item = {0};
    if (sync_mode == 0) { // POSIX Semaphore
        if (item_count == 0) {
            pthread_mutex_lock(&buffer_mutex);
            cons_blocked_count++;
            pthread_mutex_unlock(&buffer_mutex);
        }
        sem_wait(sem_full);
        pthread_mutex_lock(&buffer_mutex);
    } else { // Mutex + Condition Variable
        pthread_mutex_lock(&buffer_mutex);
        while (item_count == 0) {
            cons_blocked_count++;
            pthread_cond_wait(&cond_full, &buffer_mutex);
        }
    }

    // Critical Section
    item = buffer[out_idx];
    buffer[out_idx].id = 0; // Clear slot
    out_idx = (out_idx + 1) % buffer_capacity;
    item_count--;
    total_consumed++;
    consumed_in_interval++;

    if (sync_mode == 0) {
        pthread_mutex_unlock(&buffer_mutex);
        sem_post(sem_empty);
    } else {
        pthread_cond_signal(&cond_empty);
        pthread_mutex_unlock(&buffer_mutex);
    }

    return item;
}

// --- Worker Threads ---
void* producer_worker(void* arg) {
    int id = *(int*)arg;
    static int global_item_id = 1;

    while (1) {
        if (id < active_producers) {
            pthread_mutex_lock(&buffer_mutex);
            int current_id = global_item_id++;
            pthread_mutex_unlock(&buffer_mutex);

            BufferItem item = {
                .id = current_id,
                .producer_id = id,
                .color = producer_colors[id]
            };

            produce_item_sync(id, item);
            usleep((useconds_t)(prod_delay * 1000000.0f));
        } else {
            usleep(100000); // 100ms
        }
    }
    return NULL;
}

void* consumer_worker(void* arg) {
    int id = *(int*)arg;

    while (1) {
        if (id < active_consumers) {
            consume_item_sync(id);
            usleep((useconds_t)(cons_delay * 1000000.0f));
        } else {
            usleep(100000); // 100ms
        }
    }
    return NULL;
}

// --- Main Program ---
int main() {
    // 1. Initialize Synchronization Objects
    sem_unlink("/sem_empty_v3");
    sem_unlink("/sem_full_v3");
    sem_empty = sem_open("/sem_empty_v3", O_CREAT, 0666, MAX_BUFFER);
    sem_full  = sem_open("/sem_full_v3",  O_CREAT, 0666, 0);

    pthread_mutex_init(&buffer_mutex, NULL);
    pthread_cond_init(&cond_empty, NULL);
    pthread_cond_init(&cond_full, NULL);

    // 2. Pre-create Thread Pools
    pthread_t prod_threads[MAX_THREADS];
    pthread_t cons_threads[MAX_THREADS];
    int thread_ids[MAX_THREADS];

    for (int i = 0; i < MAX_THREADS; i++) {
        thread_ids[i] = i;
        pthread_create(&prod_threads[i], NULL, producer_worker, &thread_ids[i]);
        pthread_create(&cons_threads[i], NULL, consumer_worker, &thread_ids[i]);
    }

    // 3. Raylib UI Setup
    InitWindow(1024, 680, "Advanced Multi-Threaded Producer-Consumer Engine");
    SetTargetFPS(60);
    last_time_check = GetTime();

    while (!WindowShouldClose()) {
        double current_time = GetTime();

        // คำนวณ Throughput ทุกๆ 1 วินาที
        if (current_time - last_time_check >= 1.0) {
            throughput = (float)consumed_in_interval / (float)(current_time - last_time_check);
            consumed_in_interval = 0;
            last_time_check = current_time;
        }

        BeginDrawing();
        ClearBackground((Color){ 245, 245, 247, 255 });

        // --- Header & Mode Switcher ---
        DrawRectangle(0, 0, 1024, 60, (Color){ 30, 41, 59, 255 });
        DrawText("Producer-Consumer Multi-Threaded Engine", 20, 18, 22, WHITE);

        const char* mode_label = (sync_mode == 0) ? "Mode: POSIX Semaphore" : "Mode: Mutex + CondVar";
        Color mode_btn_col = (sync_mode == 0) ? SKYBLUE : ORANGE;
        if (GuiButton((Rectangle){ 750, 12, 250, 36 }, mode_label, mode_btn_col)) {
            pthread_mutex_lock(&buffer_mutex);
            sync_mode = 1 - sync_mode; // Toggle
            pthread_mutex_unlock(&buffer_mutex);
        }

        // --- Panel 1: Buffer Visualizer (Middle Left) ---
        DrawRectangleRounded((Rectangle){ 20, 80, 640, 360 }, 0.03f, 4, WHITE);
        DrawRectangleLinesEx((Rectangle){ 20, 80, 640, 360 }, 2.0f, LIGHTGRAY);
        DrawText("Bounded Buffer Queue", 40, 95, 20, DARKGRAY);

        pthread_mutex_lock(&buffer_mutex);

        // วาด Grid แสดงช่อง Buffer (รองรับได้สูงสุด 20 ช่อง)
        int cols = 5;
        for (int i = 0; i < buffer_capacity; i++) {
            int row = i / cols;
            int col = i % cols;
            int x = 45 + col * 115;
            int y = 135 + row * 80;

            DrawRectangleRounded((Rectangle){ x, y, 100, 65 }, 0.1f, 4, (Color){ 240, 240, 242, 255 });
            DrawRectangleLinesEx((Rectangle){ x, y, 100, 65 }, 1.5f, GRAY);
            DrawText(TextFormat("Slot %d", i), x + 8, y + 6, 12, DARKGRAY);

            if (buffer[i].id != 0) {
                // วาดสีไอคอนตามสีของ Producer ที่ผลิต
                DrawRectangleRounded((Rectangle){ x + 10, y + 24, 80, 32 }, 0.2f, 4, buffer[i].color);
                DrawText(TextFormat("#%d", buffer[i].id), x + 25, y + 30, 18, WHITE);
            }
        }

        pthread_mutex_unlock(&buffer_mutex);

        // --- Panel 2: Interactive Control Panel (Middle Right) ---
        DrawRectangleRounded((Rectangle){ 680, 80, 324, 360 }, 0.03f, 4, WHITE);
        DrawRectangleLinesEx((Rectangle){ 680, 80, 324, 360 }, 2.0f, LIGHTGRAY);
        DrawText("Control Panel", 700, 95, 20, DARKGRAY);

        // 1. Control Producers Count
        DrawText(TextFormat("Producers (N): %d", active_producers), 700, 135, 16, BLACK);
        if (GuiButton((Rectangle){ 880, 130, 35, 28 }, "-", LIGHTGRAY) && active_producers > 1) active_producers--;
        if (GuiButton((Rectangle){ 925, 130, 35, 28 }, "+", LIGHTGRAY) && active_producers < MAX_THREADS) active_producers++;

        // 2. Control Consumers Count
        DrawText(TextFormat("Consumers (M): %d", active_consumers), 700, 175, 16, BLACK);
        if (GuiButton((Rectangle){ 880, 170, 35, 28 }, "-", LIGHTGRAY) && active_consumers > 1) active_consumers--;
        if (GuiButton((Rectangle){ 925, 170, 35, 28 }, "+", LIGHTGRAY) && active_consumers < MAX_THREADS) active_consumers++;

        // 3. Control Buffer Capacity
        DrawText(TextFormat("Capacity: %d", buffer_capacity), 700, 215, 16, BLACK);
        if (GuiButton((Rectangle){ 880, 210, 35, 28 }, "-", LIGHTGRAY) && buffer_capacity > 5) {
            pthread_mutex_lock(&buffer_mutex);
            buffer_capacity--;
            pthread_mutex_unlock(&buffer_mutex);
        }
        if (GuiButton((Rectangle){ 925, 210, 35, 28 }, "+", LIGHTGRAY) && buffer_capacity < MAX_BUFFER) {
            pthread_mutex_lock(&buffer_mutex);
            buffer_capacity++;
            if (sync_mode == 0) sem_post(sem_empty);
            pthread_mutex_unlock(&buffer_mutex);
        }

        // 4. Production Speed Delay
        DrawText(TextFormat("Prod Delay: %.1fs", prod_delay), 700, 255, 16, BLACK);
        if (GuiButton((Rectangle){ 880, 250, 35, 28 }, "-", LIGHTGRAY) && prod_delay > 0.2f) prod_delay -= 0.2f;
        if (GuiButton((Rectangle){ 925, 250, 35, 28 }, "+", LIGHTGRAY) && prod_delay < 3.0f) prod_delay += 0.2f;

        // 5. Consumption Speed Delay
        DrawText(TextFormat("Cons Delay: %.1fs", cons_delay), 700, 295, 16, BLACK);
        if (GuiButton((Rectangle){ 880, 290, 35, 28 }, "-", LIGHTGRAY) && cons_delay > 0.2f) cons_delay -= 0.2f;
        if (GuiButton((Rectangle){ 925, 290, 35, 28 }, "+", LIGHTGRAY) && cons_delay < 3.0f) cons_delay += 0.2f;

        // --- Legend: Producer Thread Colors ---
        DrawText("Active Producer Threads:", 700, 335, 14, DARKGRAY);
        for (int i = 0; i < active_producers; i++) {
            DrawRectangle(700 + (i % 5) * 45, 360 + (i / 5) * 20, 35, 15, producer_colors[i]);
            DrawText(TextFormat("P%d", i), 700 + (i % 5) * 45 + 8, 360 + (i / 5) * 20 + 1, 12, WHITE);
        }

        // --- Panel 3: Performance Analytics Dashboard (Bottom) ---
        DrawRectangleRounded((Rectangle){ 20, 460, 984, 200 }, 0.02f, 4, (Color){ 30, 41, 59, 255 });
        DrawText("Real-Time Analytics & System Status", 40, 475, 20, SKYBLUE);

        pthread_mutex_lock(&buffer_mutex);
        float utilization = ((float)item_count / (float)buffer_capacity) * 100.0f;
        int current_items = item_count;
        long p_blocked = prod_blocked_count;
        long c_blocked = cons_blocked_count;
        long t_prod = total_produced;
        long t_cons = total_consumed;
        pthread_mutex_unlock(&buffer_mutex);

        // Stat Card 1: Throughput
        DrawRectangleRounded((Rectangle){ 40, 510, 210, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("THROUGHPUT", 55, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("%.1f", throughput), 55, 550, 32, GREEN);
        DrawText("items / sec", 55, 595, 14, LIGHTGRAY);

        // Stat Card 2: Buffer Utilization
        DrawRectangleRounded((Rectangle){ 270, 510, 210, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("UTILIZATION", 285, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("%.1f%%", utilization), 285, 550, 32, GOLD);
        DrawProgressBar((Rectangle){ 285, 595, 180, 15 }, utilization / 100.0f);
        DrawText(TextFormat("%d / %d slots", current_items, buffer_capacity), 285, 615, 12, LIGHTGRAY);

        // Stat Card 3: Thread Stalls / Blocked
        DrawRectangleRounded((Rectangle){ 500, 510, 220, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("THREAD BLOCKS", 515, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("Prod: %ld", p_blocked), 515, 555, 18, RED);
        DrawText(TextFormat("Cons: %ld", c_blocked), 515, 585, 18, ORANGE);

        // Stat Card 4: Total Counters
        DrawRectangleRounded((Rectangle){ 740, 510, 240, 130 }, 0.05f, 4, (Color){ 47, 63, 86, 255 });
        DrawText("TOTAL PROCESSED", 755, 525, 14, LIGHTGRAY);
        DrawText(TextFormat("Produced: %ld", t_prod), 755, 555, 18, WHITE);
        DrawText(TextFormat("Consumed: %ld", t_cons), 755, 585, 18, WHITE);

        EndDrawing();
    }

    // Cleanup
    CloseWindow();
    sem_close(sem_empty);
    sem_close(sem_full);
    sem_unlink("/sem_empty_v3");
    sem_unlink("/sem_full_v3");
    pthread_mutex_destroy(&buffer_mutex);
    pthread_cond_destroy(&cond_empty);
    pthread_cond_destroy(&cond_full);

    return 0;
}