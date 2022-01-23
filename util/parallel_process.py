from configs import job_configs as jcfg
from tqdm import tqdm
import concurrent.futures


def parallel_process(array, function, n_jobs=jcfg.WORKER, use_kwargs=False, front_num=3, use_tqdm=True):
    """
        A parallel version of the map function with a progress bar.

        Args:
            array (array-like): An array to iterate over.
            function (function): A python function to apply to the elements of array
            n_jobs (int, default=16): The number of cores to use
            use_kwargs (boolean, default=False): Whether to consider the elements of array as dictionaries of
                keyword arguments to function
            front_num (int, default=3): The number of iterations to run serially before kicking off the parallel job.
                Useful for catching bugs
        Returns:
            [function(array[0]), function(array[1]), ...]
            :param use_tqdm:
    """
    # We run the first few iterations serially to catch bugs
    front = []
    if front_num > 0:
        front = [function(**a) if use_kwargs else function(a) for a in array[:front_num]]
    # If we set n_jobs to 1, just run a list comprehension. This is useful for benchmarking and debugging.
    if n_jobs == 1:
        if use_tqdm:
            return front + [function(**a) if use_kwargs else function(a) for a in tqdm(array[front_num:])]
        else:
            return front + [function(**a) if use_kwargs else function(a) for a in array[front_num:]]
    # Assemble the workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as pool:
        # Pass the elements of array into function
        if use_kwargs:
            futures = [pool.submit(function, **a) for a in array[front_num:]]
            # futures = {pool.submit(function, **a) for a in array[front_num:]}
        else:
            # futures = [pool.submit(function, a) for a in array[front_num:]]
            futures = {pool.submit(function, a): a for a in array[front_num:]}

        # print(futures)
        kwargs = {
            'total': len(futures),
            'unit': 'it',
            'unit_scale': True,
            'leave': True,
            'ncols': 80
        }
        # Print out the progress as tasks complete
        if use_tqdm:
            for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
                pass
        else:
            for f in concurrent.futures.as_completed(futures):
                pass
    out = []
    # Get the results from the futures.
    if use_tqdm:
        for i, future in tqdm(enumerate(futures)):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
        return front + out
    else:
        for i, future in enumerate(futures):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
        return front + out





