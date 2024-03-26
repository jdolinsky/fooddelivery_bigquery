import pandas as pd
import re, os

# Quick cleanup of data

# Remove trailing colons, and special characters from items
def rm_colon_spchars(text):
    # define special characters to remove
    special = "[@_!#$%^&*()<>?/\|}{~]"
    text = re.sub(special, '', text)
    # remove trailing colons 
    if text[-1] == ":" :
        return text[:-1]
    else :
        return text

# clean up
def data_cleanup(file_path, file_name, file_dest):
    # load data
    fullpath = os.path.join(file_path, file_name)  
    df = pd.read_csv(fullpath)
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    # remove colons and convert to lower case
    df["items"] = df["items"].map(rm_colon_spchars).map(lambda t : t.lower())
    # save to csv
    outdir = os.path.join(file_path, file_dest)
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    fullpath = os.path.join(outdir, file_name)   
    df.to_csv(fullpath)

# test
if __name__ == '__main__':
    data_cleanup("mnt/files", "food_daily_2024-03-16.csv")