from amarcord.util import natural_key


def test_sort_natural() -> None:
    mylist = ["image10.png", "image1.png", "image2.png", "image20.png"]

    mylist.sort(key=natural_key)

    assert mylist == ["image1.png", "image2.png", "image10.png", "image20.png"]
