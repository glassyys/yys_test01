import tkinter as tk
from tkinter import messagebox
import math
import random
import time

class Point3D:
    """3D 점"""
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z
    
    def rotate_x(self, angle):
        """X축 기준 회전"""
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        y = self.y * cos_a - self.z * sin_a
        z = self.y * sin_a + self.z * cos_a
        return Point3D(self.x, y, z)
    
    def rotate_y(self, angle):
        """Y축 기준 회전"""
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        x = self.x * cos_a + self.z * sin_a
        z = -self.x * sin_a + self.z * cos_a
        return Point3D(x, self.y, z)
    
    def rotate_z(self, angle):
        """Z축 기준 회전"""
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        x = self.x * cos_a - self.y * sin_a
        y = self.x * sin_a + self.y * cos_a
        return Point3D(x, y, self.z)
    
    def project_2d(self, width, height, distance=300):
        """3D를 2D 평면에 투영"""
        scale = distance / (distance + self.z)
        x = int(self.x * scale + width / 2)
        y = int(-self.y * scale + height / 2)
        return x, y, scale

class Tetromino3D:
    """3D 테트로미노 블록"""
    COLORS = ['#FF0000', '#00FF00', '#0000FF', '#FFFF00', '#FF00FF', '#00FFFF', '#FFA500']
    
    def __init__(self, block_type=None):
        if block_type is None:
            block_type = random.randint(0, 6)
        
        self.block_type = block_type
        self.color = self.COLORS[block_type]
        self.points = self._create_shape()
    
    def _create_shape(self):
        """테트로미노 형태 생성"""
        shapes = [
            # I형
            [Point3D(-1.5, 0, 0), Point3D(-0.5, 0, 0), Point3D(0.5, 0, 0), Point3D(1.5, 0, 0)],
            # O형
            [Point3D(-0.5, -0.5, 0), Point3D(0.5, -0.5, 0), Point3D(-0.5, 0.5, 0), Point3D(0.5, 0.5, 0)],
            # T형
            [Point3D(-1, 0, 0), Point3D(0, 0, 0), Point3D(1, 0, 0), Point3D(0, 1, 0)],
            # L형
            [Point3D(-1, 0, 0), Point3D(0, 0, 0), Point3D(1, 0, 0), Point3D(1, 1, 0)],
            # J형
            [Point3D(-1, 0, 0), Point3D(0, 0, 0), Point3D(1, 0, 0), Point3D(-1, 1, 0)],
            # S형
            [Point3D(-1, 0, 0), Point3D(0, 0, 0), Point3D(0, 1, 0), Point3D(1, 1, 0)],
            # Z형
            [Point3D(0, 0, 0), Point3D(1, 0, 0), Point3D(-1, 1, 0), Point3D(0, 1, 0)],
        ]
        return [Point3D(p.x, p.y, p.z) for p in shapes[self.block_type]]
    
    def rotate(self, axis):
        """회전 (axis: 0=X, 1=Y, 2=Z)"""
        angle = math.pi / 6  # 30도
        new_points = []
        
        for point in self.points:
            if axis == 0:
                new_point = point.rotate_x(angle)
            elif axis == 1:
                new_point = point.rotate_y(angle)
            else:
                new_point = point.rotate_z(angle)
            new_points.append(new_point)
        
        self.points = new_points
    
    def move(self, dx, dy, dz):
        """블록 이동"""
        for point in self.points:
            point.x += dx
            point.y += dy
            point.z += dz
    
    def get_2d_coords(self, width, height):
        """2D 좌표 반환 (투영)"""
        coords = []
        for point in self.points:
            x, y, scale = point.project_2d(width, height)
            coords.append((x, y, int(10 * scale)))
        return coords

class Game3DTetris:
    """3D 테트리스 게임 (Tkinter 기반)"""
    def __init__(self, root):
        self.root = root
        self.root.title("3D Tetris")
        self.root.geometry("900x700")
        
        # 캔버스 생성
        self.canvas = tk.Canvas(root, bg='black', width=800, height=600)
        self.canvas.pack(side=tk.TOP, padx=10, pady=10)
        
        # 정보 패널
        info_frame = tk.Frame(root, bg='lightgray')
        info_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.score_label = tk.Label(info_frame, text="Score: 0", font=("Arial", 14, "bold"))
        self.score_label.pack(side=tk.LEFT, padx=10)
        
        self.status_label = tk.Label(info_frame, text="▲W: 회전(X) ▲A: 회전(Y) ▲S: 회전(Z) ◄→: 이동", font=("Arial", 10))
        self.status_label.pack(side=tk.LEFT, padx=10)
        
        # 게임 변수
        self.current_tetromino = Tetromino3D()
        self.fallen_blocks = []
        self.score = 0
        self.game_over = False
        self.fall_speed = 30  # milliseconds
        self.rotation_angle = 0
        
        # 키 바인딩
        self.root.bind('<Left>', lambda e: self.move_current(-0.5, 0, 0))
        self.root.bind('<Right>', lambda e: self.move_current(0.5, 0, 0))
        self.root.bind('<Up>', lambda e: self.move_current(0, -0.5, 0))
        self.root.bind('<Down>', lambda e: self.move_current(0, 0.5, 0))
        self.root.bind('w', lambda e: self.rotate_current(0))
        self.root.bind('a', lambda e: self.rotate_current(1))
        self.root.bind('s', lambda e: self.rotate_current(2))
        self.root.bind('q', lambda e: self.quit_game())
        
        self.canvas.focus_set()
        
        # 게임 루프 시작
        self.game_loop()
    
    def move_current(self, dx, dy, dz):
        """현재 블록 이동"""
        self.current_tetromino.move(dx, dy, dz)
    
    def rotate_current(self, axis):
        """현재 블록 회전"""
        self.current_tetromino.rotate(axis)
    
    def quit_game(self):
        """게임 종료"""
        self.root.quit()
    
    def game_loop(self):
        """게임 루프"""
        if not self.game_over:
            # 블록 낙하
            self.current_tetromino.move(0, 0.3, 0)
            
            # 바닥 충돌 검사
            collision = False
            for point in self.current_tetromino.points:
                if point.y > 8:
                    collision = True
                    break
            
            if collision:
                # 블록 고정
                for p in self.current_tetromino.points:
                    self.fallen_blocks.append({
                        'point': Point3D(p.x, p.y, p.z),
                        'color': self.current_tetromino.color
                    })
                
                self.current_tetromino = Tetromino3D()
                self.score += 10
                self.status_label.config(text=f"Score: {self.score}")
            
            # 자동 회전 (시연용)
            if random.random() < 0.02:
                self.current_tetromino.rotate(random.randint(0, 2))
            
            # 화면 그리기
            self.draw()
        
        # 다음 프레임 예약
        self.root.after(self.fall_speed, self.game_loop)
    
    def draw(self):
        """화면 그리기"""
        self.canvas.delete("all")
        
        # 배경 그리드
        self._draw_grid()
        
        # 떨어진 블록들
        for block in self.fallen_blocks:
            coords = block['point'].project_2d(800, 600)
            x, y, scale = coords
            size = int(15 * scale)
            self.canvas.create_rectangle(x - size, y - size, x + size, y + size,
                                       fill=block['color'], outline='white', width=1)
        
        # 현재 블록
        current_coords = self.current_tetromino.get_2d_coords(800, 600)
        for x, y, size in current_coords:
            self.canvas.create_oval(x - size, y - size, x + size, y + size,
                                   fill=self.current_tetromino.color, outline='white', width=2)
        
        # 제목
        self.canvas.create_text(400, 30, text="3D Tetris", font=("Arial", 20, "bold"),
                               fill='white')
    
    def _draw_grid(self):
        """배경 그리드 그리기"""
        for i in range(-6, 7, 2):
            for j in range(-6, 7, 2):
                p = Point3D(i, 8, j)
                x, y, _ = p.project_2d(800, 600)
                self.canvas.create_oval(x - 2, y - 2, x + 2, y + 2,
                                       fill='#333333', outline='#333333')

if __name__ == "__main__":
    root = tk.Tk()
    game = Game3DTetris(root)
    root.mainloop()
