# The Test Flow

Step 1: Each test is a standalone class. It declares a `clusterConfig()` method that returns a `List<ClusterConfig>` with tiered storage enabled (via `TieredStorageTestUtils.createServerPropsForRemoteStorage`), and test methods annotated with `@ClusterTemplate("clusterConfig")` that receive a `ClusterInstance` provided by the test framework.

Step 2: The test is written as a specification consisting of sequential actions and assertions. The spec for the complete test is built first using `TieredStorageTestBuilder`, which creates the "actions" to be executed.

Step 3: A `TieredStorageTestContext` is created from the `ClusterInstance` (plus any extra consumer config). The test is then executed by running each action of the spec sequentially against the context.

Step 4: The test execution stops when any of the actions throws an exception (or an assertion error).

Step 5: Clean-up for the test is performed on test exit — the `TieredStorageTestContext` is closed (it is `AutoCloseable`, typically via try-with-resources) and the test report is printed.