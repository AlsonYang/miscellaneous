#------- example 1-----
import ray

ray.init()

@ray.remote(num_return_vals=3)
def calc_stuff(parameter=None):
    # Do something.
    return 1, 2, 3


output1, output2, output3 = [], [], []

# Launch the tasks.
for j in range(10):
    id1, id2, id3 = calc_stuff.remote(parameter=j)
    output1.append(id1)
    output2.append(id2)
    output3.append(id3)

# Block until the results have finished and get the results.
output1 = ray.get(output1)
output2 = ray.get(output2)
output3 = ray.get(output3)

#------- example 2-----
@ray.remote(num_return_vals=2)
def f(x):
    time.sleep(4)
    return pd.DataFrame(data = [x**2], columns = ['square']), {'double':2*x, 'tripple':3*x}

ls_square = []
ls_multiply = []
for i in tqdm.tqdm(range(4)):
    square, double = f.remote(i)
    ls_square.append(square)
    ls_multiply.append(double)

result_square = ray.get(ls_square)

result_multiply = ray.get(ls_multiply)