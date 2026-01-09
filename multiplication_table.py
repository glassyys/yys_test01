def print_multiplication_table(num):
    """구구단 출력"""
    print(f"\n===== {num}단 =====")
    for i in range(1, 10):
        result = num * i
        print(f"{num}  {i} = {result}")

def main():
    """메인 함수"""
    for num in range(2, 10):
        print_multiplication_table(num)

if __name__ == "__main__":
    main()
