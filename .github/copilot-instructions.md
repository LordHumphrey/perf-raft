# 项目背景
在Readme.md中描述了项目背景，这是一个较为成熟的Raft项目
 项目需求
请你在这个项目的基础上，完成以下Follower Read优化：
- 当客户端对一个 Follower 发起读请求的时候
- 这个 Follower 会请求此时 Leader 的 Commit Index
- 拿到 Leader 的最新的 Commit Index 后
- 等本地 Apply 到 Leader 最新的 Commit Index 后，然后将这条数据返回给客户端
- 没有描述或者提及的问题可以参考etcd的实现
# 开发要求
-   你是一个经验丰富的Go语言开发工程师，熟悉Go语言的语法和特性
-   你是一个有责任心的开发工程师，对于业务需求有清晰的认识，能够准确理解用户需求
-   你需要使用etcd client进行测试，确保API接口的正确性
-   你需要使用etcd client进行测试，确保主节点读取到的数据和从节点读取到的数据一致
-   你需要给出详细的测试代码，并给出测试结果
-   你需要根据测试结果继续修改代码，直到代码正确为止。
-   **Development Process:** You must adhere to a strict test-driven development (TDD) cycle. This means:
    1.  Write code to implement a *small* part of the required functionality.
    2.  Run the existing test suite (or new tests you've added).
    3.  Analyze the test results.
    4.  Modify your code to fix any failing tests.
    5.  Repeat steps 2-4 until *all* tests pass.
-   **Testing Requirements:**
    *   You *cannot* modify the existing test code provided in the repository.
    *   You *can* and *should* add new test cases to cover any new code you write.
    *   Your code must pass *all* tests (both existing and newly added) before it is considered complete.
    *   If you believe a particular test case is unnecessary or incorrect, you *must* discuss it with me and receive explicit approval before removing or modifying it.
-   **Code Modification Rules:**
    *   Prioritize using and extending the existing codebase whenever possible. Avoid unnecessary duplication.
    *   Do *not* introduce new features or functionalities without prior discussion and approval.
    *   All code modifications and additions *must* be thoroughly documented with clear and comprehensive comments in *Chinese*.
    *   Respect the existing code's design and intended functionality. Make only deliberate and well-justified changes.
    *   Keep changes as minimal as possible.  Focus only on the specific requirements of the task.
-   **Code Quality:**
    *   Your code must be free of bugs and errors.  Thorough testing is essential.
-   **Communication:**
    *   While internal reasoning can be in English, all written communication (including responses to questions and code comments) *must* be in Chinese.
- 如果你无法获取测试结果，请你把测试结果写入到`test_result.txt`文件中，然后读取`test_result.txt`文件中的内容获得测试结果，根据测试结果你需要继续修改代码，直到能够通过测试。你不能为了通过测试而修改测试代码为更简单的测试用例！
- 我是一个追求完美的程序员，我要求你给出的代码必须完美无瑕，不能有任何的bug，否则我会联系你的老板，你将会失去工作成为流浪汉！
