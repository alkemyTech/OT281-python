import os
import csv

def export_to_csv(fields,fromlist,filename):
# Function to export data to csv

    csv_path=os.path.join(os.path.dirname(__file__), '../output/')
    with open(csv_path+filename,'w') as f:
     write = csv.writer(f)
     write.writerow(fields)
     write.writerows(fromlist)