print("=== 반복 계산기 ===")
print("종료하려면 연산자에 q 입력")

while True:
    # 첫 번째 숫자 입력
    num1 = input("첫 번째 숫자를 입력하세요: ")
    if num1.lower() == 'q':
        print("계산기를 종료합니다.")
        break

    num1 = float(num1)

    # 두 번째 숫자 입력
    num2 = float(input("두 번째 숫자를 입력하세요: "))

    # 연산자 입력
    operator = input("산술 연산자를 입력하세요 (+, -, *, /) 또는 q로 종료: ")

    if operator.lower() == 'q':
        print("계산기를 종료합니다.")
        break

    # 계산 처리
    if operator == "+":
        result = num1 + num2
    elif operator == "-":
        result = num1 - num2
    elif operator == "*":
        result = num1 * num2
    elif operator == "/":
        if num2 == 0:
            print("오류: 0으로 나눌 수 없습니다.")
            continue
        result = num1 / num2
    else:
        print("오류: 올바른 연산자를 입력하세요.")
        continue

    # 결과 출력
    print("결과:", result)
    print("-" * 30)