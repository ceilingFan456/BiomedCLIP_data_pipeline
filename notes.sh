## link to data on datadisk
lsblk -o NAME,HCTL,SIZE,MOUNTPOINT
sudo mkdir -p /mnt/data
sudo mount /dev/sda1 /mnt/data
df -h | grep /mnt/data





python make_webdataset_shards.py \
  --input_jsonl /home/azureuser/BiomedCLIP_data_pipeline/_results/data/pubmed_parsed_data.jsonl \
  --out_dir     /mnt/datadisk/biomedclip_wds/train \
  --prefix      biomedclip-train \
  --shard_size  10000 \
  --allowlist_file /home/azureuser/BiomedCLIP_data_pipeline/_results/data/pubmed_open_access_file_list_train.txt


mkdir -p /mnt/data/pmc/{train,dev,test}/{compressed,extracted,logs}


python process.py \
  --mode devtest \
  --dev_list  /home/azureuser/disk/_results/data/pubmed_open_access_file_list_dev.txt \
  --test_list /home/azureuser/disk/_results/data/pubmed_open_access_file_list_test.txt \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like


python process.py \
  --mode shards \
  --shard_dir /home/azureuser/disk/_results/data/shards_train \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like


python process_parallel.py \
  --mode shards \
  --shard_dir /home/azureuser/disk/_results/data/shards_train \
  --out_root  /home/azureuser/disk/_results/data/pmc15_cc12m_like \
  --num_workers 8

