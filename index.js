const sdk = require('node-appwrite');
const { InputFile } = require('node-appwrite/file');
var mime = require('mime-lite');

module.exports = async ({ req, res, log, error }) => {
    // 1. 初始化 SDK (使用内部变量或环境变量)
    const client = new sdk.Client()
        .setEndpoint(process.env.APPWRITE_FUNCTION_ENDPOINT || 'https://sgp.cloud.appwrite.io/v1')
        .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
        .setKey(process.env.APPWRITE_API_KEY); // 需要在函数设置里添加该环境变量

    const storage = new sdk.Storage(client);
    const databases = new sdk.Databases(client);

    const BUCKET_ID = '697d59bf0006e5ac97fe';
    const DB_ID = 's3_metadata';
    const COLL_ID = 'file_mapping';

    // 2. 解析路由 (模拟 S3 路径: /bucket/filename)
    // 1. 获取原始路径，例如: "/mybucket/hello.txt"
    const rawPath = req.path;

    // 2. 切割路径并过滤掉空字符串
    // ["mybucket", "hello.txt"]
    const pathParts = rawPath.split('/').filter(part => part !== '');

    // 3. 手动模拟 params
    const bucket = pathParts[0];
    const filename = pathParts.length > 1 ? pathParts.slice(1).join('/') : null;

    log(`[解析结果] Bucket: ${bucket}, Filename: ${filename || '空 (准备列出列表)'}`);
    if (!bucket && !filename) {
        return res.send('Error: Missing bucket or filename in path. Usage: /bucket or /bucket/file', 400);
    }

    try {
        // --- 处理 PUT (上传/覆盖) ---
        if (req.method === 'PUT' && bucket && filename) {
            log(`[PUT] 正在上传: ${filename}`);

            // 检查重复并清理 (逻辑同前)
            const existing = await databases.listDocuments(DB_ID, COLL_ID, [
                sdk.Query.equal('filename', filename)
            ]);
            for (const doc of existing.documents) {
                await storage.deleteFile(BUCKET_ID, doc.fileId);
                await databases.deleteDocument(DB_ID, COLL_ID, doc.$id);
            }

            const binaryData = req.bodyBinary; // 理想情况下它应该是 Buffer 或 Uint8Array


            log(`[UPLOAD] 接收到数据长度: ${binaryData.length}`);

            // 云函数中的 req.body 已经是 Buffer 或二进制格式
            const fileToUpload = InputFile.fromBuffer(binaryData, filename);
            const uploadedFile = await storage.createFile(BUCKET_ID, sdk.ID.unique(), fileToUpload);

            await databases.createDocument(DB_ID, COLL_ID, sdk.ID.unique(), {
                filename,
                fileId: uploadedFile.$id,
                bucketName: bucket,
                size: binaryData.length // 记录精确到字节的大小
            });

            return res.send(`OK: ${uploadedFile.$id}`, 200);
        }

        // --- 处理 GET (下载) ---
        if (req.method === 'GET' && bucket && filename) {
            const result = await databases.listDocuments(DB_ID, COLL_ID, [
                sdk.Query.equal('filename', filename)
            ]);
            if (result.total === 0) return res.send('Not Found', 404);

            const fileBuffer = await storage.getFileDownload(BUCKET_ID, result.documents[0].fileId);
            // 注意：Appwrite 函数返回二进制需要处理
            const contentType = mime.getType(filename) || 'application/octet-stream';
            if (contentType.includes(';')) {
                contentType = contentType.split(';')[0];
            }
            const buffer = Buffer.from(fileBuffer);
            log(`[DEBUG] 准备发送数据，长度为: ${fileBuffer.byteLength} 字节`);
            const headers = {
                'Content-Type': contentType, 'Content-Length': fileBuffer.byteLength,
                'Content-Transfer-Encoding': 'binary',
                'Content-Disposition': `attachment; filename="${filename}"`, 'Cache-Control': 'public, max-age=3600'
            }
            // res.headers = {};
            return res.send(buffer, 200, headers
                //   { 'Content-Type': 'binary/octet-stream' }
            );
        }
        if (req.method === 'GET' && bucket && !filename) {


            console.log(`[列表] 正在查询桶: ${bucket}`);
            log(`[LIST] 正在生成 S3 XML 列表...`);

            // 1. 从数据库获取所有文件映射
            const result = await databases.listDocuments(DB_ID, COLL_ID, [
                sdk.Query.equal('bucketName', bucket)
            ]);

            // 2. 构造 S3 风格的 XML (可选) 或 极简 JSON
            // 构建 S3 风格的 XML 字符串
            const contentsXml = result.documents.map(doc => `
                        <Contents>
                            <Key>${doc.filename}</Key>
                            <LastModified>${doc.$createdAt}</LastModified>
                            <Size>${doc.size || 0}</Size>
                            <StorageClass>STANDARD</StorageClass>
                        </Contents>`).join('');

            const xmlResponse = `<?xml version="1.0" encoding="UTF-8"?>
                    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                        <Name>${bucket}</Name>
                        <Prefix></Prefix>
                        <Marker></Marker>
                        <MaxKeys>1000</MaxKeys>
                        <IsTruncated>false</IsTruncated>
                        ${contentsXml}
                    </ListBucketResult>`;

            return res.send(xmlResponse, 200, {
                'Content-Type': 'application/xml'
            });
        }

        // --- 处理 DELETE ---
        if (req.method === 'DELETE' && bucket && filename) {
            // ... 删除逻辑 ...

            //     const { bucket, filename } = req.params;
            console.log(`[删除] 正在处理: ${filename}`);

            // 1. 先去数据库查到这个文件名对应的 fileId
            const result = await databases.listDocuments(DB_ID, COLL_ID, [
                sdk.Query.equal('filename', filename),
                sdk.Query.equal('bucketName', bucket)
            ]);

            if (result.total === 0) {
                return res.send('File not found in mapping', 404);
            }

            const docId = result.documents[0].$id;
            const fileId = result.documents[0].fileId;

            // 2. 双重删除：先删存储文件，再删数据库记录
            // 注意：顺序很重要，如果先删了记录但文件删除失败，你会失去对文件的引用
            await storage.deleteFile(BUCKET_ID, fileId);
            await databases.deleteDocument(DB_ID, COLL_ID, docId);

            return res.send('Deleted', 204);
        }

        return res.send('Invalid Request', 400);

    } catch (err) {
        error(err.message);
        return res.send('Internal Error: ' + err.message, 500);
    }
};