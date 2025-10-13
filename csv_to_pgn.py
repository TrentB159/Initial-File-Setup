import os
import pandas as pd
import chess.pgn
from chess import Board

# input and output directory, must be changed upon use
input_dir = "simulated_games_filtered_CSV"  
output_dir = "simulated_games_filtered_PGN"
os.makedirs(output_dir, exist_ok=True)

# batch size
games_per_file = 1000
current_batch = []
file_count = 1
game_counter = 1

# write a batch of games
def write_batch(games, index):
    batch_path = os.path.join(output_dir, f"games_batch_{index:03}.pgn")
    with open(batch_path, "w") as f:
        for g in games:
            f.write(str(g) + "\n\n")

# loop the files, collect a row at a time and save it
# write it out once we have enough games
for file in os.listdir(input_dir):
    if file.startswith("part-") and file.endswith(".csv"):
        df_path = os.path.join(input_dir, file)
        
        df = pd.read_csv(df_path)


        for _, row in df.iterrows():
            moves_str = row.get("Moves")

            # split up the moves string
            tokens = moves_str.strip().split()
            moves_san = [tok for tok in tokens if not tok.endswith(".")]
            
            # make a game 
            board = Board()
            game = chess.pgn.Game()
            node = game

            # write in games headers
            game.headers["Event"] = f"Game {game_counter}"
            game.headers["Site"] = file
            game.headers["White"] = str(row.get("White", "Unknown"))
            game.headers["Black"] = str(row.get("Black", "Unknown"))
            game.headers["Result"] = str(row.get("Result", "*"))
            
            # manually "play" the game and write it
            for san in moves_san:
                move = board.parse_san(san)
                board.push(move)
                node = node.add_variation(move)
                
            current_batch.append(game)
            game_counter += 1

            if len(current_batch) >= games_per_file:
                write_batch(current_batch, file_count)
                file_count += 1
                current_batch = []


# Write any remaining games
if current_batch:
    write_batch(current_batch, file_count)
