# conf-pull

## 功能
```bash
1. 将配置从git仓库拉取到业务所在服务器；
2. 支持比较远端仓库与本地目录的文件级别的区别，增量重载配置；
3. 支持dns域名的本机缓存，避免因为dns故障导致服务不可用；
4. 支持git日志/ssh日志及conf-pull业务日志，方便问题定位；
5. 支持定期重置本地git仓库，避免仓库越来越大；
```

## 使用
```bash
sh ./control start/stop/restart/status
```
