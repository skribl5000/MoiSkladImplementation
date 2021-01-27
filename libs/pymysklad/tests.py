
def test_import():
    try:
        import pymyskald
        result = True

    except ImportError:
        result = False

    assert result
