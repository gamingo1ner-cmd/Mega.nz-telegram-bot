from services.storage_service import file_hash

def test_hash():
    assert file_hash("requirements.txt")
