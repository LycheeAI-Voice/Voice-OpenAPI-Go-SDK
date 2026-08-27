# Samples

统一命令行示例已内置服务地址：HTTP/WebSocket 使用 `https://voice.lycheeai.net/openapi`，gRPC 使用 `voice.lycheeai.net:46060`。用户只需填写 API Key，并在命令中提供 speaker ID。

## Windows PowerShell

1. 进入本目录，并将模板复制为本地配置（仓库已为当前本地工作区创建 `.env`）：

   ```powershell
   cd D:\workspace\voice-openapi-go-sdk-publish\samples
   Copy-Item .env.example .env
   ```

2. 打开 `.env`，将 `VOICE_OPENAPI_API_KEY` 和 `VOICE_OPENAPI_SPEAKER_ID` 改成自己的值。`.env` 已被 Git 忽略，不会提交。

3. 配置 Go 到当前 PowerShell 会话并执行 WebSocket 示例：

   ```powershell
   $env:Path = 'C:\Users\admin\tools\go\bin;' + $env:Path
   go run . -transport ws -speaker 清远
   ```

   成功后会在当前目录生成 `output.mp3`。若 `.env` 已填写 `VOICE_OPENAPI_SPEAKER_ID`，可省略 `-speaker`。

## 其他传输方式

```powershell
go run . -transport http -speaker 清远
go run . -transport grpc -speaker 清远
```

可选参数：`-text "要合成的文本"`、`-output custom.mp3`。请勿将真实 API Key 写入或提交到版本库。
