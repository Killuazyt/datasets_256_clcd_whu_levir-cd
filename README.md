# datasets_256_clcd_whu_levir-cd
用于changevit的数据集切分，自己使用

## 第一步下载
```https://huggingface.co/datasets/ericyu/LEVIRCD_Cropped256```

```https://huggingface.co/datasets/ericyu/CLCD_Cropped_256```

```https://www.dropbox.com/scl/fi/8gczkg78fh95yofq5bs7p/WHU.zip?rlkey=05bpczx0gdp99hl6o2xr1zvyj&dl=0```

第三个是可以直接使用的
## 切分
因为huggingface不让直连，而且我也需要图片的形式来使用，这里需要对其进行格式转换。

parquet_to_images.py 是测试用的 直接使用另外两个即可
自己修改路径即可

```python parquet_to_images_levir.py```

```parquet_to_images_clcd.py```

## 整理好的上传链接
暂时还没有去做
