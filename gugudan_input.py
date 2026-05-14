"""
구구단 계산기: 단과 값을 입력받아 곱셈 결과를 출력합니다.
"""

def main():
    try:
        # 단 입력 받기
        dan = int(input("구구단 단을 입력하세요 (2-9): "))

        # 값 입력 받기
        value = int(input("값을 입력하세요 (1-9): "))

        # 유효성 검사
        if not (2 <= dan <= 9):
            print("단은 2부터 9까지 입력해주세요.")
            return

        if not (1 <= value <= 9):
            print("값은 1부터 9까지 입력해주세요.")
            return

        # 계산 및 결과 출력
        result = dan * value
        print(f"{dan} × {value} = {result}")

    except ValueError:
        print("숫자를 입력해주세요.")
    except Exception as e:
        print(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()