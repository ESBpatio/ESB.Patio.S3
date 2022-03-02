using System;
using System.Threading;
using ESB_ConnectionPoints.PluginsInterfaces;
using ESB_ConnectionPoints.Utils;
using Newtonsoft.Json.Linq;
using Amazon;
using Amazon.S3.Model;
using System.IO;

namespace ESB.Patio.S3
{
    class OutgoingConnectionPoint : IStandartOutgoingConnectionPoint
    {
        private readonly ILogger logger;
        private int timeOut;
        private string bucketName, serviceURL, accessKey, secretKey;
        private bool debugMode;
        private Amazon.S3.AmazonS3Client s3Client;
        public OutgoingConnectionPoint(string jsonSettings, IServiceLocator serviceLocator)
        {
            ParseSetting(jsonSettings);
            logger = new Logger(serviceLocator.GetLogger(GetType()), debugMode, "S3 client");
            if (string.IsNullOrEmpty(jsonSettings))
                throw new Exception("Не найдены настройки исходящей точки");           
        }
        public void ParseSetting(string settings)
        {
            JObject jObject;
            try
            {
                jObject = JObject.Parse(settings);
            }
            catch (Exception ex)
            {

                throw new Exception("Не удалось разобрать строку настроек! Ошибка : " + ex.Message);
            }
            debugMode = JsonUtils.BoolValue(jObject, "debugMode");
            timeOut = JsonUtils.IntValue(jObject, "timeOut", 5);
            bucketName = JsonUtils.StringValue(jObject, "bucketName");
            serviceURL = JsonUtils.StringValue(jObject, "serviceURL");
            accessKey = JsonUtils.StringValue(jObject, "accessKey");
            secretKey = JsonUtils.StringValue(jObject, "secretKey");
        }
        public void Run(IMessageSource messageSource, IMessageReplyHandler replyHandler, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                Message message = null;
                try
                {
                    message = messageSource.PeekLockMessage(ct, 10000);
                }
                catch (Exception ex)
                {
                    logger.Error(string.Format("Ошибка получения сообщения из очереди. \n Описание ошибки : {0}", ex.Message));
                }
                if (message == null)
                    continue;
                try
                {
                    switch (message.Type)
                    {
                        case "DTP":
                            MemoryStream ms = new MemoryStream(message.Body, 0, message.Body.Length);
                            PutObjectRequest request = new PutObjectRequest
                            {
                                BucketName = this.bucketName,
                                Key = "Datareon.jpg",
                                InputStream = ms
                            };
                            PutObjectResponse response = s3Client.PutObject(request);
                            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
                                CompletePeeklock(logger, messageSource, message.Id, MessageHandlingError.UnknowError, "Не удалось сохранить файл в хранилище");                          
                            break;
                        default:
                            CompletePeeklock(logger, messageSource, message.Id, MessageHandlingError.InvalidMessageType, "Данный тип сообщения не поддерживается");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    CompletePeeklock(logger, messageSource, message.Id, MessageHandlingError.RejectedMessage, ex.Message);
                }
                CompletePeeklock(logger, messageSource, message.Id);
            }
        }
        public void Cleanup()
        {
        }
        public void Dispose()
        {
        }
        public void Initialize()
        {
            try
            {
                s3Client = TryConnectS3();
                if (!CheckBucket())
                    throw new Exception("На клиенте не найден bucket с именем " + this.bucketName);
            }
            catch (Exception ex)
            {

                throw new ApplicationException(string.Format("Произошла ошибка при соединение с хранилищем S3. \n Описание ошибки : {0}",ex.Message));
            }
            
        }
        public void CompletePeeklock(ILogger logger, IMessageSource messageSource, Guid Id, MessageHandlingError messageHandlingError, string errorMessage)
        {
            logger.Error(string.Format("Ошибка отправки сообщения , подробности : {0}", errorMessage));
            messageSource.CompletePeekLock(Id, messageHandlingError, errorMessage);

        }
        public void CompletePeeklock(ILogger logger, IMessageSource messageSource, Guid Id)
        {
            messageSource.CompletePeekLock(Id);
            logger.Debug("Сообщение обработано");
        }
        public Amazon.S3.AmazonS3Client TryConnectS3()
        {
            Amazon.S3.AmazonS3Config config = new Amazon.S3.AmazonS3Config()
            {
                RegionEndpoint = RegionEndpoint.USEast1,
                ServiceURL = serviceURL,
                AllowAutoRedirect = true,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromMinutes(timeOut)
            };
            return new Amazon.S3.AmazonS3Client(accessKey, secretKey, config);
        }
        public bool CheckBucket()
        {
            ListBucketsResponse listBucketsResponse = s3Client.ListBuckets();
            foreach (var bucket in listBucketsResponse.Buckets)
            {
                if (bucket.BucketName == this.bucketName)
                    return true;
            }
            return false;
        }

    }
}
