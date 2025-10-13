import chess.pgn
import pandas as pd

# parse the PGN file 
def parse_pgn_file(filename):
    games = []
    with open(filename, encoding='utf-8') as f:
        while True:
            # read a game 
            game = chess.pgn.read_game(f)
            if game is None:
                break 
            # get the game data
            games.append({
                #comment in any that you want
                "White": game.headers.get("White", ""),
                "Black": game.headers.get("Black", ""),
                "WhiteElo": game.headers.get("WhiteElo", ""),
                "BlackElo": game.headers.get("BlackElo", ""),
                "Result": game.headers.get("Result", ""),
                "Date": game.headers.get("Date", ""), 
                "Event": game.headers.get("Event", ""), 
                "ECO": game.headers.get("ECO", ""),
                "Moves": game.board().variation_san(game.mainline_moves())
            })
            
    return games

def main():
    # put any pgn files names you want here
    # This must be changed accordingly
    pgn_files = [
        "simulated_games_batch_1.pgn", "simulated_games_batch_2.pgn", "simulated_games_batch_3.pgn",
        "simulated_games_batch_4.pgn", "simulated_games_batch_5.pgn", "simulated_games_batch_6.pgn",
        "simulated_games_batch_7.pgn", "simulated_games_batch_8.pgn", "simulated_games_batch_9.pgn",
        "simulated_games_batch_10.pgn", "simulated_games_batch_11.pgn", "simulated_games_batch_12.pgn",
        "simulated_games_batch_13.pgn", "simulated_games_batch_14.pgn", "simulated_games_batch_15.pgn",
        "simulated_games_batch_16.pgn", "simulated_games_batch_17.pgn", "simulated_games_batch_18.pgn",
        "simulated_games_batch_19.pgn", "simulated_games_batch_20.pgn"
    ] 
    # write to CSV 
    for file in pgn_files:
        games = parse_pgn_file(file)
        df = pd.DataFrame(games)
        csv_filename = file.replace(".pgn", ".csv")
        df.to_csv(csv_filename, index=False)

if __name__ == "__main__":
    main()
