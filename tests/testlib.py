def setup_testdir(testname):
    """
    prepare a test directory
    """
    import os
    import shutil
    import __init__

    if testname == None or testname == "":
        raise Exception("testname is required")

    testdir = os.path.join("/tmp", "anomdec_tests", testname)
    if os.path.exists(testdir):
        shutil.rmtree(testdir)
    os.makedirs(testdir)

    return testdir

