from myetl.f_load_data import f_load_data

def test_load():
    r = f_load_data('2025/03/12/00')
    assert "파일이 생성되었습니다" in r
