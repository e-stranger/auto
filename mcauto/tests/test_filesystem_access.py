import os


def test_filesystem_access():
    path = r"\\laxfpsp03101\dfs\mcm\client\Adidas"
    assert os.path.exists(path) == True