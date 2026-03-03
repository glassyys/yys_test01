import random
import tkinter as tk

BOARD_W = 10
BOARD_H = 20
CELL = 30
SIDE_W = 140
DROP_MS = 500

COLORS = {
    "I": "#00bcd4",
    "O": "#ffd54f",
    "T": "#ba68c8",
    "S": "#81c784",
    "Z": "#e57373",
    "J": "#64b5f6",
    "L": "#ffb74d",
}

SHAPES = {
    "I": [[(-1, 0), (0, 0), (1, 0), (2, 0)], [(0, -1), (0, 0), (0, 1), (0, 2)]],
    "O": [[(0, 0), (1, 0), (0, 1), (1, 1)]],
    "T": [
        [(-1, 0), (0, 0), (1, 0), (0, 1)],
        [(0, -1), (0, 0), (1, 0), (0, 1)],
        [(-1, 0), (0, 0), (1, 0), (0, -1)],
        [(0, -1), (0, 0), (-1, 0), (0, 1)],
    ],
    "S": [[(-1, 1), (0, 1), (0, 0), (1, 0)], [(0, -1), (0, 0), (1, 0), (1, 1)]],
    "Z": [[(-1, 0), (0, 0), (0, 1), (1, 1)], [(1, -1), (1, 0), (0, 0), (0, 1)]],
    "J": [
        [(-1, 0), (0, 0), (1, 0), (1, 1)],
        [(0, -1), (0, 0), (0, 1), (1, -1)],
        [(-1, -1), (-1, 0), (0, 0), (1, 0)],
        [(-1, 1), (0, -1), (0, 0), (0, 1)],
    ],
    "L": [
        [(-1, 0), (0, 0), (1, 0), (-1, 1)],
        [(0, -1), (0, 0), (0, 1), (1, 1)],
        [(-1, 0), (0, 0), (1, 0), (1, -1)],
        [(-1, -1), (0, -1), (0, 0), (0, 1)],
    ],
}


class Tetris:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Simple Tetris")
        self.canvas = tk.Canvas(root, width=BOARD_W * CELL + SIDE_W, height=BOARD_H * CELL, bg="#111", highlightthickness=0)
        self.canvas.pack()

        self.board = [[None for _ in range(BOARD_W)] for _ in range(BOARD_H)]
        self.score = 0
        self.lines = 0
        self.level = 1
        self.running = True

        self.current = None
        self.next_kind = random.choice(list(SHAPES.keys()))

        self.spawn_piece()
        self.draw()

        self.root.bind("<Left>", lambda e: self.move(-1, 0))
        self.root.bind("<Right>", lambda e: self.move(1, 0))
        self.root.bind("<Down>", lambda e: self.soft_drop())
        self.root.bind("<Up>", lambda e: self.rotate())
        self.root.bind("<space>", lambda e: self.hard_drop())
        self.root.bind("r", lambda e: self.restart())

        self.tick()

    def spawn_piece(self):
        kind = self.next_kind
        self.next_kind = random.choice(list(SHAPES.keys()))
        self.current = {"kind": kind, "rot": 0, "x": BOARD_W // 2, "y": 1}
        if self.collision(0, 0, 0):
            self.running = False

    def get_cells(self, piece=None, dx=0, dy=0, drot=0):
        p = self.current if piece is None else piece
        kind = p["kind"]
        rotations = SHAPES[kind]
        rot = (p["rot"] + drot) % len(rotations)
        base = rotations[rot]
        return [(p["x"] + x + dx, p["y"] + y + dy) for x, y in base]

    def collision(self, dx, dy, drot):
        for x, y in self.get_cells(dx=dx, dy=dy, drot=drot):
            if x < 0 or x >= BOARD_W or y >= BOARD_H:
                return True
            if y >= 0 and self.board[y][x] is not None:
                return True
        return False

    def move(self, dx, dy):
        if not self.running:
            return
        if not self.collision(dx, dy, 0):
            self.current["x"] += dx
            self.current["y"] += dy
            self.draw()

    def soft_drop(self):
        if not self.running:
            return
        if not self.collision(0, 1, 0):
            self.current["y"] += 1
            self.score += 1
        else:
            self.lock_piece()
        self.draw()

    def hard_drop(self):
        if not self.running:
            return
        dropped = 0
        while not self.collision(0, 1, 0):
            self.current["y"] += 1
            dropped += 1
        self.score += dropped * 2
        self.lock_piece()
        self.draw()

    def rotate(self):
        if not self.running:
            return
        if not self.collision(0, 0, 1):
            self.current["rot"] = (self.current["rot"] + 1) % len(SHAPES[self.current["kind"]])
        else:
            for kick in (-1, 1, -2, 2):
                if not self.collision(kick, 0, 1):
                    self.current["x"] += kick
                    self.current["rot"] = (self.current["rot"] + 1) % len(SHAPES[self.current["kind"]])
                    break
        self.draw()

    def lock_piece(self):
        color = COLORS[self.current["kind"]]
        for x, y in self.get_cells():
            if y >= 0:
                self.board[y][x] = color
        self.clear_lines()
        self.spawn_piece()

    def clear_lines(self):
        new_board = [row for row in self.board if any(cell is None for cell in row)]
        cleared = BOARD_H - len(new_board)
        if cleared:
            for _ in range(cleared):
                new_board.insert(0, [None for _ in range(BOARD_W)])
            self.board = new_board
            self.lines += cleared
            self.score += [0, 100, 300, 500, 800][cleared] * self.level
            self.level = 1 + self.lines // 10

    def tick(self):
        if self.running:
            if not self.collision(0, 1, 0):
                self.current["y"] += 1
            else:
                self.lock_piece()
            self.draw()
        delay = max(80, DROP_MS - (self.level - 1) * 35)
        self.root.after(delay, self.tick)

    def restart(self):
        self.board = [[None for _ in range(BOARD_W)] for _ in range(BOARD_H)]
        self.score = 0
        self.lines = 0
        self.level = 1
        self.running = True
        self.next_kind = random.choice(list(SHAPES.keys()))
        self.spawn_piece()
        self.draw()

    def draw_cell(self, x, y, color):
        x1 = x * CELL
        y1 = y * CELL
        x2 = x1 + CELL
        y2 = y1 + CELL
        self.canvas.create_rectangle(x1, y1, x2, y2, fill=color, outline="#222")

    def draw_board(self):
        for y in range(BOARD_H):
            for x in range(BOARD_W):
                c = self.board[y][x]
                if c:
                    self.draw_cell(x, y, c)
                else:
                    self.canvas.create_rectangle(x * CELL, y * CELL, (x + 1) * CELL, (y + 1) * CELL, outline="#222")

    def draw_piece(self):
        if not self.running and self.current is None:
            return
        color = COLORS[self.current["kind"]]
        for x, y in self.get_cells():
            if y >= 0:
                self.draw_cell(x, y, color)

    def draw_side_panel(self):
        sx = BOARD_W * CELL + 12
        self.canvas.create_text(sx, 30, text="TETRIS", anchor="nw", fill="#fafafa", font=("Consolas", 16, "bold"))
        self.canvas.create_text(sx, 80, text=f"Score: {self.score}", anchor="nw", fill="#ddd", font=("Consolas", 12))
        self.canvas.create_text(sx, 110, text=f"Lines: {self.lines}", anchor="nw", fill="#ddd", font=("Consolas", 12))
        self.canvas.create_text(sx, 140, text=f"Level: {self.level}", anchor="nw", fill="#ddd", font=("Consolas", 12))

        self.canvas.create_text(sx, 190, text="Next:", anchor="nw", fill="#ddd", font=("Consolas", 12))
        kind = self.next_kind
        color = COLORS[kind]
        preview = SHAPES[kind][0]
        for px, py in preview:
            cx = sx + 40 + px * 16
            cy = 230 + py * 16
            self.canvas.create_rectangle(cx, cy, cx + 14, cy + 14, fill=color, outline="#222")

        self.canvas.create_text(
            sx,
            330,
            text="Left/Right: Move\nUp: Rotate\nDown: Soft drop\nSpace: Hard drop\nR: Restart",
            anchor="nw",
            justify="left",
            fill="#aaa",
            font=("Consolas", 10),
        )

        if not self.running:
            self.canvas.create_rectangle(40, 230, BOARD_W * CELL - 40, 320, fill="#000", outline="#e57373", width=2)
            self.canvas.create_text(BOARD_W * CELL // 2, 260, text="GAME OVER", fill="#e57373", font=("Consolas", 20, "bold"))
            self.canvas.create_text(BOARD_W * CELL // 2, 295, text="Press R to restart", fill="#ddd", font=("Consolas", 11))

    def draw(self):
        self.canvas.delete("all")
        self.draw_board()
        if self.current:
            self.draw_piece()
        self.draw_side_panel()


def main():
    root = tk.Tk()
    root.resizable(False, False)
    Tetris(root)
    root.mainloop()


if __name__ == "__main__":
    main()
