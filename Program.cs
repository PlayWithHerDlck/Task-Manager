using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace InteractiveTaskManager
{
    #region 1. СЛОЙ ЗАВИСИМОСТЕЙ (DI) И ПАТТЕРН СТРАТЕГИЯ

    public interface ILogger
    {
        void Log(string message, ConsoleColor color = ConsoleColor.Gray);
    }

    public interface IStorageStrategy
    {
        ValueTask SaveAsync<T>(T data);
    }

    // Реализация интерактивного логгера, который пишет в выделенную зону экрана
    public class InteractiveLogger : ILogger
    {
        private static readonly object ConsoleLock = new();

        public void Log(string message, ConsoleColor color = ConsoleColor.Gray)
        {
            lock (ConsoleLock)
            {
                // Запоминаем где стоял курсор ввода пользователя
                int left = Console.CursorLeft;
                int top = Console.CursorTop;

                // Переносим курсор в зону логов (с 10-й строки)
                Console.SetCursorPosition(0, 12);
                Console.ForegroundColor = color;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}".PadRight(Console.WindowWidth - 1));
                Console.ResetColor();

                // Возвращаем курсор обратно для корректного отображения меню
                Console.SetCursorPosition(left, top);
            }
        }
    }

    public class DatabaseStorageStrategy : IStorageStrategy
    {
        private readonly ILogger _logger;
        public DatabaseStorageStrategy(ILogger logger) => _logger = logger;

        public ValueTask SaveAsync<T>(T data)
        {
            _logger.Log($"[STRATEGY] Данные '{data}' успешно сохранены в СУБД.", ConsoleColor.DarkGreen);
            return ValueTask.CompletedTask;
        }
    }

    #endregion

    #region 2. ОПТИМИЗИРОВАННОЕ ЯДРО (Generics, ValueTask, ConcurrentQueue)

    public enum TaskStatus { Created, Running, Completed, Failed }

    public class TaskContext<T>
    {
        public Guid Id { get; } = Guid.NewGuid();
        public T Payload { get; }
        public TaskStatus Status { get; set; } = TaskStatus.Created;

        public TaskContext(T payload) => Payload = payload;
    }

    public class TaskBroker<T>
    {
        private readonly ConcurrentQueue<TaskContext<T>> _taskQueue = new();
        private readonly List<Task> _workerTasks = new();
        private readonly IStorageStrategy _storageStrategy;
        private readonly ILogger _logger;
        private CancellationTokenSource? _cts;

        // DI-конструктор
        public TaskBroker(IStorageStrategy storageStrategy, ILogger logger)
        {
            _storageStrategy = storageStrategy;
            _logger = logger;
        }

        public int QueueCount => _taskQueue.Count;

        // Добавление задачи через Span без лишних аллокаций строк
        public void AddTaskFromUi(ReadOnlySpan<char> commandName, T payload)
        {
            var context = new TaskContext<T>(payload);
            _taskQueue.Enqueue(context);
            _logger.Log($"[PRODUCER] Добавлена задача {context.Id.ToString().Substring(0,8)}: Команда [{commandName.ToString()}]", ConsoleColor.Cyan);
        }

        public void StartWorkers(int workerCount)
        {
            _cts = new CancellationTokenSource();
            for (int i = 0; i < workerCount; i++)
            {
                int workerId = i + 1;
                _workerTasks.Add(Task.Run(() => WorkerLoopAsync(workerId, _cts.Token)));
            }
            _logger.Log($"[SYSTEM] Запущено асинхронных воркеров: {workerCount}", ConsoleColor.Blue);
        }

        private async Task WorkerLoopAsync(int workerId, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_taskQueue.TryDequeue(out var context))
                {
                    context.Status = TaskStatus.Running;
                    _logger.Log($"[WORKER {workerId}] Взял задачу {context.Id.ToString().Substring(0,8)} в работу...", ConsoleColor.Yellow);

                    // Высокочастотный вызов через ValueTask
                    await ProcessTaskAsync(context, workerId, cancellationToken);
                }
                else
                {
                    // Ожидание без утилизации процессора
                    await Task.Delay(100, cancellationToken);
                }
            }
        }

        // Оптимизированный по памяти метод обработки
        private async ValueTask ProcessTaskAsync(TaskContext<T> context, int workerId, CancellationToken token)
        {
            try
            {
                // Имитация бурной деятельности воркера (2.5 секунды)
                await Task.Delay(2500, token);

                // Сохранение через стратегию
                await _storageStrategy.SaveAsync(context.Payload);

                context.Status = TaskStatus.Completed;
                _logger.Log($"[WORKER {workerId}] Успешно ВЫПОЛНИЛ задачу {context.Id.ToString().Substring(0,8)}!", ConsoleColor.Green);
            }
            catch (OperationCanceledException)
            {
                _logger.Log($"[WORKER {workerId}] Прерван во время работы над {context.Id.ToString().Substring(0,8)}", ConsoleColor.Red);
            }
            catch (Exception ex)
            {
                _logger.Log($"[ERROR] Ошибка воркера: {ex.Message}", ConsoleColor.Red);
            }
        }

        public async Task StopWorkersAsync()
        {
            _logger.Log("[SYSTEM] Инициирована graceful-остановка. Отмена токенов...", ConsoleColor.Magenta);
            _cts?.Cancel();

            try
            {
                await Task.WhenAll(_workerTasks);
            }
            catch (OperationCanceledException) { }
            finally
            {
                _cts?.Dispose();
                _logger.Log("[SYSTEM] Все воркеры полностью и безопасно остановлены.", ConsoleColor.Magenta);
            }
        }
    }

    #endregion

    #region 3. ИНТЕРФЕЙС И УПРАВЛЕНИЕ СТРЕЛОЧКАМИ

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Clear();
            Console.CursorVisible = false;

            // Настройка DI Контейнера
            var serviceProvider = new ServiceCollection()
                .AddSingleton<ILogger, InteractiveLogger>()
                .AddSingleton<IStorageStrategy, DatabaseStorageStrategy>()
                .AddSingleton(typeof(TaskBroker<>))
                .BuildServiceProvider();

            var broker = serviceProvider.GetRequiredService<TaskBroker<string>>();
            var logger = serviceProvider.GetRequiredService<ILogger>();

            // Запускаем 2 параллельных воркера
            broker.StartWorkers(workerCount: 2);

            var scenarios = new[]
            {
                "Собрать Docker-контейнер",
                "Выгрузить транзакции в СУБД",
                "Очистить кэш статических файлов",
                "Сгенерировать квартальный PDF отчет"
            };
            int selectedIndex = 0;
            bool isRunning = true;

            // Основной интерактивный цикл UI
            while (isRunning)
            {
                RenderMenu(scenarios, selectedIndex, broker.QueueCount);

                var key = Console.ReadKey(true);
                switch (key.Key)
                {
                    case ConsoleKey.UpArrow:
                        selectedIndex = selectedIndex == 0 ? scenarios.Length - 1 : selectedIndex - 1;
                        break;

                    case ConsoleKey.DownArrow:
                        selectedIndex = selectedIndex == scenarios.Length - 1 ? 0 : selectedIndex + 1;
                        break;

                    case ConsoleKey.Enter:
                        // Парсинг через ReadOnlySpan прямо "из воздуха" (AsSpan)
                        ReadOnlySpan<char> cmdHeader = "CMD_GENERIC".AsSpan();
                        broker.AddTaskFromUi(cmdHeader, scenarios[selectedIndex]);
                        break;

                    case ConsoleKey.Escape:
                        isRunning = false;
                        break;
                }
            }

            // Безопасное закрытие системы по кнопке Esc
            Console.SetCursorPosition(0, 10);
            await broker.StopWorkersAsync();
            Console.CursorVisible = true;
            Console.WriteLine("\nПрограмма завершена. Нажмите любую клавишу для закрытия окна...");
            Console.ReadKey();
        }

        private static void RenderMenu(string[] scenarios, int selectedIndex, int currentQueueCount)
        {
            lock (typeof(Console))
            {
                Console.SetCursorPosition(0, 0);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("=== ИНТЕРФЕЙС УПРАВЛЕНИЯ БРОКЕРОМ ЗАДАЧ ===");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine("Управление: [Стрелочки Вверх/Вниз] - Выбор | [Enter] - Добавить задачу | [Esc] - Выход");
                Console.WriteLine($"Задач в очереди прямо сейчас: {currentQueueCount} ".PadRight(Console.WindowWidth - 1));
                Console.WriteLine(new string('-', Console.WindowWidth - 1));

                for (int i = 0; i < scenarios.Length; i++)
                {
                    if (i == selectedIndex)
                    {
                        Console.BackgroundColor = ConsoleColor.Gray;
                        Console.ForegroundColor = ConsoleColor.Black;
                        Console.WriteLine($">  {scenarios[i]} ".PadRight(Console.WindowWidth - 1));
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.WriteLine($"   {scenarios[i]} ".PadRight(Console.WindowWidth - 1));
                    }
                }
                Console.WriteLine(new string('-', Console.WindowWidth - 1));
                Console.SetCursorPosition(0, 11);
                Console.ForegroundColor = ConsoleColor.DarkGray;
                Console.WriteLine("--- ЖИВОЙ ЛОГ ОБРАБОТКИ СИСТЕМЫ (CORE) ---");
                Console.ResetColor();
            }
        }
    }

    #endregion
}
