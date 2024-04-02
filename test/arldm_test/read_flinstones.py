import random
import time
import hydra

import cv2
import h5py
import numpy as np
import torch
from torch.utils.data import Dataset
from torchvision import transforms
from transformers import CLIPTokenizer
from omegaconf import DictConfig

from torch.utils.data import DataLoader

from models.blip_override.blip import init_tokenizer

# Copied fromn flintstones.py

class StoryDataset(Dataset):
    """
    A custom subset class for the LRW (includes train, val, test) subset
    """

    def __init__(self, subset, args):
        super(StoryDataset, self).__init__()
        self.args = args

        self.h5_file = args.get(args.dataset).hdf5_file
        self.subset = subset

        self.augment = transforms.Compose([
            transforms.ToPILImage(),
            transforms.Resize([512, 512]),
            transforms.ToTensor(),
            transforms.Normalize([0.5], [0.5])
        ])
        self.dataset = args.dataset
        self.max_length = args.get(args.dataset).max_length
        self.clip_tokenizer = CLIPTokenizer.from_pretrained('runwayml/stable-diffusion-v1-5', subfolder="tokenizer")
        self.blip_tokenizer = init_tokenizer()
        msg = self.clip_tokenizer.add_tokens(list(args.get(args.dataset).new_tokens))
        print("clip {} new tokens added".format(msg))
        msg = self.blip_tokenizer.add_tokens(list(args.get(args.dataset).new_tokens))
        print("blip {} new tokens added".format(msg))

        self.blip_image_processor = transforms.Compose([
            transforms.ToPILImage(),
            transforms.Resize([224, 224]),
            transforms.ToTensor(),
            transforms.Normalize([0.48145466, 0.4578275, 0.40821073], [0.26862954, 0.26130258, 0.27577711])
        ])

    def __getitem__(self, index):
        
        if hasattr(self, 'h5'):
            Exception("h5 file not opened")
        h5 = h5py.File(self.h5_file, "r")
        self.h5 = h5[self.subset]

        images = list()
        for i in range(5):
            im = self.h5['image{}'.format(i)][index]
            im = cv2.imdecode(im, cv2.IMREAD_COLOR)
            idx = random.randint(0, 4)
            images.append(im[idx * 128: (idx + 1) * 128])

        source_images = torch.stack([self.blip_image_processor(im) for im in images])
        images = images[1:] if self.args.task == 'continuation' else images
        images = torch.stack([self.augment(im) for im in images]) \
            if self.subset in ['train', 'val'] else torch.from_numpy(np.array(images))

        texts = self.h5['text'][index].decode('utf-8').split('|')

        # tokenize caption using default tokenizer
        tokenized = self.clip_tokenizer(
            texts[1:] if self.args.task == 'continuation' else texts,
            padding="max_length",
            max_length=self.max_length,
            truncation=False,
            return_tensors="pt",
        )
        captions, attention_mask = tokenized['input_ids'], tokenized['attention_mask']

        tokenized = self.blip_tokenizer(
            texts,
            padding="max_length",
            max_length=self.max_length,
            truncation=False,
            return_tensors="pt",
        )
        source_caption, source_attention_mask = tokenized['input_ids'], tokenized['attention_mask']
        return images, captions, attention_mask, source_images, source_caption, source_attention_mask

    def __len__(self):

        if hasattr(self, 'h5'):
            Exception("h5 file not opened")
        h5 = h5py.File(self.h5_file, "r")
        self.h5 = h5[self.subset]
        return len(self.h5['text'])

    def __get_dset_size__(self):
        if hasattr(self, 'h5'):
            Exception("h5 file not opened")
        h5 = h5py.File(self.h5_file, "r")
        self.h5 = h5[self.subset]
        datasets = self.h5.keys()
        for dset in datasets:
            print(f"Length of h5_file[{self.subset}][{dset}]: {len(self.h5[dset])}")
    
    def __get_dset_item__(self, index):
        if hasattr(self, 'h5'):
            Exception("h5 file not opened")
        h5 = h5py.File(self.h5_file, "r")
        self.h5 = h5[self.subset]
        datasets = self.h5.keys()
        return_items = []
        for dset in datasets:
            return_items.append(self.h5[dset][index])
        return return_items

    # function to close file
    def close(self):
        self.h5.close()
        del self.h5

def read_items_in_data(data_name, args):
    data = StoryDataset(data_name, args)
    print(f"Length of {data_name}: {len(data)}")
    data.__get_dset_size__()
    for i in range(0, len(data), 20):
        data.__get_dset_item__(i)

def load_data(args: DictConfig) -> None:
    datasets_names = ["image0", "image1", "image2", "image3", "image4"]
    
    print("In def load_data()")
    
    # print h4 file
    print("h5_file:", args.get(args.dataset).hdf5_file)
    
    start_time_seconds = time.time()
    
    
    
    read_items_in_data("train", args)
    read_items_in_data("val", args)
    read_items_in_data("test", args)
    
        
    # print("Loading test_data...")
    # test_data = StoryDataset("test", args)
    # print("Length of test_data:", len(test_data))
    # # Get shape of the dset
    # test_data.__get_dset_size__()
    # # Get every 20th item
    # for i in range(0, len(test_data), 20):
    #     test_data.__getitem__(i)
    
    end_time_seconds = time.time()
    print("Import data start time (sec):", start_time_seconds)
    print("Import data end time (sec):", end_time_seconds)
    # Calculate the time difference
    time_elapsed = end_time_seconds - start_time_seconds
    # Calculate minutes and seconds from the time difference
    minutes_elapsed = int(time_elapsed // 60)
    seconds_elapsed = int(time_elapsed % 60)
    print(f"Time elapsed: {time_elapsed:.2f} seconds ({minutes_elapsed} minutes and {seconds_elapsed} seconds)")


@hydra.main(config_path=".", config_name="config")

def main(args: DictConfig) -> None:

    print("In def main()")
    load_data(args)
    

if __name__ == "__main__":
    main()
    
