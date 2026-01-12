import getpass

def main():
    user_id = getpass.getuser() # 현재 OS 사용자 이름 가져오기 
    print(f"{user_id}님 안녕하세요")

if __name__ == "__main__":
    main()
