import pandas as pd
import argparse



def compute_flows(out_acc,in_acc, shortlist_percentage = 20):

    flows = [] # from id, to id, flow
    shortlist_number = 5

    # out_acc will be of the form [amount of people to move, x, y]
    # in_acc will be of the form [capacity, x, y]
    dnf = []
    for i,out in enumerate(out_acc):
        for j,into in enumerate(in_acc):
            distance = (out[1] - into[1]) ** 2 + (out[2] - into[2]) ** 2
            distance = (distance)**0.5
            dnf.append([i, j, distance, out[0], into[0]])


    out = pd.DataFrame(out_acc)
    into = pd.DataFrame(in_acc)
    into.sort_values(by=0, ascending=True, inplace=True)
    into_idx = list(into.index)

    for id in into_idx:
        capacity_r = in_acc[id][0]
        x_r = in_acc[id][1]
        y_r = in_acc[id][2]
        data = []
        for i,out in enumerate(out_acc):
            distance = ((out[1] - x_r)**2 + (out[2] - y_r)**2)**0.5
            if out[0] > 0:
                data.append([i, distance, out[0], min(capacity_r,out[0])])
        if len(data) == 0:
            break

        df = pd.DataFrame(data)
        df.sort_values(by = 1, ascending = True, inplace = True)



        while capacity_r > 0:
            if df.values.shape[0] == 0:
                break
            closest = df.iloc[0][1]
            sub = df.loc[df[1] <= (100 + shortlist_percentage)/100*closest,:]
            sub = sub.loc[sub[3] > 0,:]
            sub.sort_values(by = 3, ascending= False, inplace= True)
            max_inflow = sub[3].sum()

            if max_inflow <= capacity_r: # if the location can fit all shortlisted flows do so
                out_ids = list(sub[0])
                out_flows = list(sub[3])

                for i,o in enumerate(out_ids):
                    flows.append([o,id,out_flows[i]]) #output flows
                    out_acc[o][0] -= out_flows[i] #reduce amount of people at outgoing location

                capacity_r -= max_inflow #reduce capacity
                drop_idx = sub.index
                df.drop(index = drop_idx,inplace=True) #remove used locations
                df[3] = df[3].apply(lambda x: min(x,capacity_r)) #update main dataframe with new capacity


            else: #if the location cannot fit all flows go through the sorted list one by one and update the one location that cannot fully fit

                for row in sub:
                    sub_index = sub.index
                    flow = sub.iloc[row][3]
                    outgoing_id = int(sub.iloc[row][0])
                    if flow < capacity_r: # if the location can accomodate the flow
                        capacity_r -= flow #reduce capacity
                        #df.drop(index = sub_index[row], inplace = True) #remove used location
                        flows.append([outgoing_id,id,flow]) #output flows
                        out_acc[outgoing_id][0] -= flow  # reduce amount of people at outgoing location
                    else:
                        actual_flow = capacity_r
                        capacity_r = 0

                        outgoing_location_count = sub.iloc[row][2]
                        #if outgoing_location_count == actual_flow:
                            #df.drop(index=sub_index[row], inplace=True)  # remove used location
                        flows.append([outgoing_id, id, actual_flow])  # output flows
                        out_acc[outgoing_id][0] -= actual_flow  # reduce amount of people at outgoing location
                        break


    return flows,out_acc

parser = argparse.ArgumentParser(description='parser')
parser.add_argument("-send", "--send_filename", help="Name of input file for send locations", type= str)
parser.add_argument("-receive", "--receive_filename", help="Name of input file for receive locations", type= str)
parser.add_argument("-percentage", "--percentage", help="Shortlist percentage number between 0 and 100. High percentage -> less care about distance.", type= float)
parser.add_argument("-d", "--delimiter", help="delimiter used for files", type= str, default=',')
args = parser.parse_args()

send_file = args.send_filename
receive_file = args.receive_filename
percentage = args.percentage


if send_file is None:
    out_acc = pd.read_csv('send2.csv', header = None, skiprows=1, delimiter = args.delimiter)
else:
    out_acc = pd.read_csv(send_file, header = None, skiprows=1, delimiter = args.delimiter)

out_names = list(out_acc[0])
out_acc.drop(columns=0, inplace = True)
out_acc_list = out_acc.values.tolist()

if receive_file is None:
    in_acc = pd.read_csv('receive2.csv', header = None, skiprows=1, delimiter = args.delimiter)
else:
    in_acc = pd.read_csv(receive_file, header = None, skiprows=1, delimiter = args.delimiter)

in_names = list(in_acc[0])
print(len(in_names))
print(len(in_acc))
in_acc.drop(columns=0, inplace = True)
in_acc_list = in_acc.values.tolist()

if percentage is None:
    flows,out_acc_result = compute_flows(out_acc_list,in_acc_list)
else:
    flows, out_acc_result = compute_flows(out_acc_list, in_acc_list, percentage)

flows_header = ['from name', 'from index', 'to name', 'to index', 'flow']



final_flows = []

for flow in flows:
    from_idx = flow[0]
    to_idx = flow[1]
    value = flow[2]
    from_name = out_names[from_idx]
    to_name = in_names[to_idx]
    final_flows.append([from_name,from_idx,to_name,to_idx,value])

final_send = []
for i,send in enumerate(out_acc_result):
    final_send.append([out_names[i]] + send)

send_header = ['name', 'people to move', 'x', 'y']
send_df = pd.DataFrame(final_send)
send_df.to_csv('updated_send.csv', header = send_header, index = None, sep = args.delimiter)
flows_df = pd.DataFrame(final_flows)
flows_df.to_csv('Final Flows.csv', header=flows_header, index=None, sep = args.delimiter)

flows_df.to_csv()
print(flows)