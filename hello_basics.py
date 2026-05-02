"""
hello.py 흐름을 확장한 예제: 파이썬 기본 문법 5가지를 한 프로그램에서 확인합니다.
실행: python hello_basics.py
"""

import getpass

# 1) 변수, 문자열, f-string
APP_NAME = "Hello Basics"


def greet(name: str) -> str:
    """4) 함수 정의, 매개변수, 반환값"""
    return f"{name}님, {APP_NAME}에 오신 것을 환영합니다."


def score_message(score: int) -> str:
    """2) 조건문 if / elif / else"""
    if score >= 90:
        return "우수"
    elif score >= 70:
        return "양호"
    else:
        return "복습 권장"


def syntax_demo_scores() -> list[int]:
    """5) 리스트 리터럴과 타입 힌트"""
    return [95, 72, 55]


def main() -> None:
    user = getpass.getuser()

    # 3) for 반복문, enumerate
    items = {"사용자": user, "모듈": __name__}
    print(greet(user))
    print("--- 문법 체크 ---")

    for key, value in items.items():
        print(f"  {key}: {value}")

    scores = syntax_demo_scores()
    for i, s in enumerate(scores, start=1):
        label = score_message(s)
        print(f"  샘플 점수 {i}: {s}점 → {label}")

    # 간단한 런타임 검증 (문법·동작이 기대와 같음을 확인)
    assert score_message(100) == "우수"
    assert score_message(70) == "양호"
    assert score_message(0) == "복습 권장"
    assert greet("테스트").startswith("테스트님")
    print("--- assert 통과 ---")


if __name__ == "__main__":
    main()
