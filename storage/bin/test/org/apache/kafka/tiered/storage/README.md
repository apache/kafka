# The Test Flow

Step 1: For every test, setup is done via TieredStorageTestHarness which extends IntegrationTestHarness and sets up a cluster with TS enabled on it.

Step 2: The test is written as a specification consisting of sequential actions and assertions. The spec for the complete test is written down first which creates "actions" to be executed.

Step 3: Once we have the test spec in-place (which includes assertion actions), we execute the test which will execute each action sequentially. 

Step 4: The test execution stops when any of the action throws an exception (or an assertion error).

Step 5: Clean-up for the test is performed on test exit