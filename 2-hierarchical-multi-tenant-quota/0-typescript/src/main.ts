import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    await app.listen(3000, "0.0.0.0")
    // eslint-disable-next-line no-console
    console.log("hierarchical-quota-service listening on :3000")
}

void bootstrap()
