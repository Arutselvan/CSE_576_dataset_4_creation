# Author: Arut Selvan Dhanasekaran

from utils import Utils
from constants import warc_path_file,segments,cc_base_url
import os

WARC_LIMIT = 10

utils = Utils()
warc_file_paths = utils.get_warc_file_paths(segments,warc_path_file)
warc_count=0

for segment in segments:
    img_data = []
    print("\nProcessing segment %s\n" % segment)
    # utils.create_dir("segments/"+segment)
    # print("\nSegment %s has %d warc files\n" % (segment, len(warc_file_paths[segment])))
    for warc in warc_file_paths[segment]:
        path = warc.split('/')
        warc_file = path[-1]
        path = '/'.join(path[:len(path)-1])
        utils.create_dir(path)
        utils.download_warc(cc_base_url,warc,warc)
        # Remove the plus from the line below if processing all warc files in batches
        img_data += utils.process_warc(warc,segment)
        # utils.write_to_csv(img_data,"segments/"+ segment + "/" + warc_file.replace('.','_')+ '.csv')
        # images_folder = "segments/" + segment + "/" + warc_file.replace('.','_') + "_images"
        # utils.create_dir(images_folder)
        # utils.download_images(img_data, images_folder)
        os.remove(warc) # deletes .warc file after processing it.

# Lines commented above can be used for processing all warc files in all segments in batches

# Lines below can be used to generate sample data for testing filters, etc.

        warc_count+=1
        if warc_count>=WARC_LIMIT:
            break

    img_folder = 'sample_images'
    utils.create_dir(img_folder)
    failed_images = utils.download_images(img_data,img_folder)
    for img in img_data:
        if img['uuid'] in failed_images:
            img_data.remove(img)
    print(len(img_data))
    utils.write_to_csv(img_data,'sample_data.csv')