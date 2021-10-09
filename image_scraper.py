from urllib.parse import urljoin
from threading import Thread
from queue import Queue
import requests
import sys, os
from progressbar import ProgressBar

class Worker(Thread):
    """
    Thread executing tasks from a given tasks queue
    """

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except:
                # An exception happened in this thread
                pass
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """
    Pool of threads consuming tasks from a queue
    """

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """
        Add a task to the queue
        """

        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """
        Add a list of tasks to the queue
        """

        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """
        Wait for completion of all the tasks in the queue
        """

        self.tasks.join()


class ImgScrapy:
    """
    Image scraper class
    """

    def __init__(self,directory):
        self.img_count = 0
        self.downloaded_links = []
        self.failed_links = []
        self.directory = directory
        self.download_directory = directory
        self.processed_count = 0
        self.max_threads = 50
        self.failed_images = []

    def download_img(self, image_link, file_location,pb,uuid):
        """
        Method to download an image
        """

        if '?' in file_location:
            file_location = file_location.split('?')[-2]
            
        try:
            image_request = requests.get(image_link, stream=True)
            if image_request.status_code == 200:
                with open(file_location, 'wb') as f:
                    f.write(image_request.content)
                self.downloaded_links.append(image_link)
                self.processed_count+=1
                pb.update(self.processed_count)
            else:
                # self.failed_links.append(image_link)
                self.failed_images.append(uuid)
                self.processed_count+=1
                pb.update(self.processed_count)
            # print("downloaded")
        except:
            self.failed_images.append(uuid)
            self.failed_links.append(image_link)
            self.processed_count+=1
            pb.update(self.processed_count)

    def download_images(self,links):
        """
        Method to download images given a list of urls
        """

        if not os.path.exists(self.download_directory):
            try:
                os.makedirs(self.download_directory)
            except:
                print("Directory not found")

        self.img_links = links

        self.img_count = len(self.img_links)
        pb = ProgressBar(maxval=self.img_count).start()

        pool_size = min(self.img_count, self.max_threads)
        pool = ThreadPool(pool_size)

        for link in self.img_links:
            file_location = self.download_directory+"/" + \
                link['img_file']
            # print(file_location)
            pool.add_task(self.download_img, link['img_link'], file_location,pb, link['uuid'])

        pool.wait_completion()
        pb.finish()
        return self.failed_images
