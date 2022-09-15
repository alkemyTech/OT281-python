
def export_to_csv(fields,fromlist,filename):
# Function to export data to csv

    import os
    
    import pandas as pd

    csv_path=os.path.join(os.path.dirname(__file__), '../output/')
    
    df = pd.DataFrame(fromlist,columns=fields)
    df.to_csv(csv_path+filename, index=False)

