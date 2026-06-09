import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    const port = Number(process.env.PORT ?? 3000)
    await app.listen(port, "0.0.0.0")
    // eslint-disable-next-line no-console
    console.log(`token-bucket-service listening on :${port}`)
}

void bootstrap()
