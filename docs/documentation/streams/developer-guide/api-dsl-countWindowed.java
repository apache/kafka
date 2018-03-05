import java.util.concurrent.TimeUnit;
KGroupedStream<String, Long> groupedStream = ...;

// Counting a KGroupedStream with time-based windowing (here: with 5-minute tumbling windows)
KTable<Windowed<String>, Long> aggregatedStream = groupedStream.windowedBy(
    TimeWindows.of(TimeUnit.MINUTES.toMillis(5))) /* time-based window */
    .count();

// Counting a KGroupedStream with session-based windowing (here: with 5-minute inactivity gaps)
KTable<Windowed<String>, Long> aggregatedStream = groupedStream.windowedBy(
    SessionWindows.with(TimeUnit.MINUTES.toMillis(5))) /* session window */
    .count();
