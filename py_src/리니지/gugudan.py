# 원하는 단만 출력하는 구구단 프로그램

dan = int(input("출력할 단을 입력하세요(2~9): "))

if 2 <= dan <= 9:
    print(f"[{dan}단]")
    for i in range(1, 10):
        print(f"{dan} x {i} = {dan * i}")
else:
    print("2부터 9 사이의 숫자를 입력해주세요.")
