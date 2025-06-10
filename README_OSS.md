# OSS Setup

This branch provides setup to run tests with Alibaba Cloud OSS.
We use an open-access OSS client -- [Jindo SDK](https://github.com/aliyun/alibabacloud-jindodata/blob/latest/docs/user/en/jindosdk/jindosdk_quickstart.md).
we adopt the standard zone-redundant storage, which is the vendor-recommended configuration.

OSS setup has been installed in the pre-build image:

```shell
docker pull alexyinhan/flink-community:flink-remote-compaction-20250606
```

Add the following configs to run tests:
```shell
fs.oss.endpoint: http://{your-region}.oss.aliyuncs.com
fs.oss.accessKeyId: {your-ak}
fs.oss.accessKeySecret: {your-sk}
fs.oss.region-id: {your-region}
```
