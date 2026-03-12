import multiprocessing
from dataloop import dataloopMain
from ruralMarketInstance import completeCall



def first_instance():
    dataloopMain()
    print('creating dataloop for generic cases')

def second_instance():
    completeCall()
    print('creating data loop for rural market cases.')

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    # creating processes
    p1 = multiprocessing.Process(target=first_instance)
    p2 = multiprocessing.Process(target=second_instance)

    # starting process 1
    p1.start()
    # starting process 2
    p2.start()

    # wait until process 1 is finished
    p1.join()
    # wait until process 2 is finished
    p2.join()

    # both processes finished
    print("Done!")


