import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Custom GUI Application build using Java AWT to visualize the skyline objects in the grid.
 */
public class RenderWindow extends JFrame implements Runnable {

    private int[][] binaryGrid;
    private List<int[]> result = new ArrayList<>();

    private boolean debug = false;

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
    }

    private void readOutputFile(String filePath) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                String[] output = strCurrentLine.trim().split("\\s+");
                if (output.length == 2) {
                    int row = Integer.parseInt(output[1]);
                    int col = Integer.parseInt(output[0]);
                    result.add(new int[]{row - 1, col - 1});
                } else {
                    throw new RuntimeException();
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    private void readFile(String filePath, String[] args) {
        try {
            int gridSize = Integer.parseInt(args[0]);
            binaryGrid = new int[gridSize][gridSize];
            for (int i = 0; i < gridSize; i++) Arrays.fill(binaryGrid[i], 0);

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String strCurrentLine;
            int row = 0;
            while ((strCurrentLine = br.readLine()) != null) {
                String[] process = strCurrentLine.split("\\s+");
                if (process.length == 3) {
                    if (row == 0 && debug) System.out.println(process[0]);
                    for (int i = 0; i < process[2].length(); i++) {
                        if (process[0].indexOf("-") != -1) {
                            if (process[2].charAt(i) == '1') {
                                binaryGrid[row][i] = -1;
                            }
                        } else {
                            if (process[2].charAt(i) == '1') {
                                binaryGrid[row][i] = 1;
                            }
                        }
                    }
                } else {
                    System.out.println("not found");
                }
                row++;
                if (row == gridSize)
                    row = 0;
            }
            System.out.println("Read the file closing the file pointer");
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (RuntimeException exception) {
            exception.printStackTrace();
        }
    }

    public RenderWindow(String[] args, String filename, String outputFIle) {
        setTitle("Area Skyline Grid");
        setSize(1920, 1080);
        readFile(filename, args);
        // readOutputFile(outputFIle);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel gridPanel = new JPanel(new GridLayout(binaryGrid.length, binaryGrid[0].length, 2, 2));

        for (int row = 0; row < binaryGrid.length; row++) {
            for (int col = 0; col < binaryGrid[row].length; col++) {
                JLabel cellLabel = new JLabel();
                cellLabel.setHorizontalAlignment(JLabel.CENTER);
                cellLabel.setVerticalAlignment(JLabel.CENTER);
                cellLabel.setOpaque(true);

                if (binaryGrid.length <= (1 << 8)){
                    if (binaryGrid[row][col] == 1) {
                        cellLabel.setBackground(Color.GREEN);
                    } else if (binaryGrid[row][col] == -1) {
                        cellLabel.setBackground(Color.RED);
                    } else {
                        cellLabel.setBackground(Color.WHITE);
                    }

                    cellLabel.setBorder(BorderFactory.createLineBorder(Color.BLACK));
                    cellLabel.setText("(" + (row + 1) + "," + (col + 1) + ")");

                    // Add a mouse listener to change the color on click
                    cellLabel.addMouseListener(new MouseAdapter() {
                        @Override
                        public void mouseClicked(MouseEvent e) {
                            cellLabel.setBackground(Color.ORANGE);
                        }
                    });
                }else {
                    cellLabel.setText(String.valueOf(col));
                }

                gridPanel.add(cellLabel);
            }
        }

        System.out.println("Components added"); // Add this line

        add(gridPanel);
        pack();
        setLocationRelativeTo(null);

        System.out.println("Frame properties set"); // Add
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            RenderWindow example = new RenderWindow(args, "input.txt", "part-00000");
            example.setVisible(true);
        });
    }
}
