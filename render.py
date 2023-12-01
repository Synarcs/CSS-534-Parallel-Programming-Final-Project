import tkinter as tk

def generate_grid():
    for i in range(8):
        for j in range(8):
            cell = tk.Label(root, text="", width=5, height=2, relief="solid", borderwidth=1, bg="white", highlightthickness=0)
            cell.grid(row=i, column=j)

# Create the main window
root = tk.Tk()
root.title("8x8 Grid Generator")

# Set the border color to black
root.configure(bg="black")

# Generate the fixed 8x8 grid
generate_grid()

# Run the Tkinter event loop
root.mainloop()
