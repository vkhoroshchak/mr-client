python client.py --mf "mapper_pd.py" --rf "reducer_pd.py" --dest "output_spotify_data.csv" --kd "0,1:2" --s
rc "..\client_data\spotify_data_f.csv" --sql "SELECT 'Track Name', Streams, Position FROM data.csv"
