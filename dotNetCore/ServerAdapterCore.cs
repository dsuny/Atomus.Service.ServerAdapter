using Atomus.Control;
using Atomus.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;

namespace Atomus.Service
{
    public class ServerAdapter : IService, IServiceAsync
    {
        private readonly bool ByPassAll;
        private readonly bool DuplicateLogin;
        private readonly Dictionary<string, string> ByPassList;
        private readonly string[] Authentication;
        private readonly string SessionMode;

        private readonly Dictionary<string, DateTime> Token;
        private readonly Dictionary<string, string> Login;

        private readonly string ExpiredCertificateMessage;
        private readonly string UnauthenticatedUserMessage;

        private IAction log;

        public ServerAdapter()
        {
            string tmp;
            string[] tmps;
            string[] tmps1;

            this.ByPassList = new Dictionary<string, string>();
            this.Token = new Dictionary<string, DateTime>();
            this.Login = new Dictionary<string, string>();

            try
            {
                this.ByPassAll = this.GetAttributeBool("ByPassAll");
            }
            catch (Exception ex)
            {
                this.ByPassAll = true;
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                if (!this.ByPassAll)
                {
                    tmp = this.GetAttribute("ByPassList");

                    if (tmp != null && tmp != "" && tmp.Contains(",") && tmp.Contains("/"))
                    {
                        tmps = tmp.Split(',');

                        foreach (string keyValue in tmps)
                        {
                            tmps1 = keyValue.Split('/');

                            if (tmps1.Length == 2)
                                this.ByPassList.Add(tmps1[0], tmps1[1]);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                if (!this.ByPassAll)
                {
                    tmp = this.GetAttribute("Authentication");

                    this.Authentication = tmp.Split(',');
                }
            }
            catch (Exception ex)
            {
                this.Authentication = null;
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                if (!this.ByPassAll)
                {
                    this.SessionMode = this.GetAttribute("SessionMode");
                }
            }
            catch (Exception ex)
            {
                this.SessionMode = null;
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                if (!this.ByPassAll)
                {
                    this.DuplicateLogin = this.GetAttributeBool("DuplicateLogin");
                }
            }
            catch (Exception ex)
            {
                this.DuplicateLogin = true;
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                this.ExpiredCertificateMessage = this.GetAttribute("ExpiredCertificateMessage");
            }
            catch (Exception ex)
            {
                this.ExpiredCertificateMessage = "Expired certificate.";
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                this.UnauthenticatedUserMessage = this.GetAttribute("UnauthenticatedUserMessage");
            }
            catch (Exception ex)
            {
                this.ExpiredCertificateMessage = "Unauthenticated user.";
                DiagnosticsTool.MyTrace(ex);
            }

            try
            {
                this.log = (IAction)this.CreateInstance("Log");
            }
            catch (Exception ex)
            {
                DiagnosticsTool.MyTrace(ex);
            }
        }

        Response IService.Request(ServiceDataSet serviceDataSet)
        {
            IResponse response;
            string tmp;
            Dictionary<string, object> logData;
            System.Text.StringBuilder stringBuilder;
            //IService service;

            response = null;
            logData = null;

            try
            {
                if (this.log != null)
                    try
                    {
                        logData = new Dictionary<string, object>();
                        logData["START_DATETIME"] = DateTime.Now;

                        ((IServiceDataSet)serviceDataSet).CreateServiceDataTable();

                        stringBuilder = new System.Text.StringBuilder();

                        this.ReadServiceDataSet(serviceDataSet, stringBuilder);
                        logData["REQUEST_BODY"] = stringBuilder.ToString();
                        stringBuilder.Clear();
                        stringBuilder = null;
                        logData["USER_ID"] = $"{(serviceDataSet as IServiceDataSet).GetAttribute("USER_ID")}";
                        logData["REQUESTER"] = $"{(serviceDataSet as IServiceDataSet).GetAttribute("REQUESTER")}";
                        logData["IP_ADDRESS"] = this.GetClientIPAddress();
                    }
                    catch (Exception) { }

                //service = (IService)Factory.CreateInstance(((IServiceDataSet)serviceDataSet).ServiceName);
                //_Service = (IService)Factory.CreateInstance(@"E:\Work\Project\Atomus\Service\DefaultService\bin\Debug\Atomus.Service.DefaultService.V1.0.0.0.dll", "Atomus.Service.DefaultService", true, true);


                //((IServiceDataSet)serviceDataSet).DataTables.Contains()

                ((IServiceDataSet)serviceDataSet).CreateServiceDataTable();

                if (this.ByPassAll)//무조건 처리
                {
                    if (((IServiceDataSet)serviceDataSet).DataTables.Contains("Token") && ((IServiceDataSet)serviceDataSet)["Token"].ConnectionName == "Atomus")
                        ((IServiceDataSet)serviceDataSet).DataTables.Remove("Token");

                    response = ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);

                    return (Response)response;
                }
                else
                {
                    if (this.Authentication != null && this.Authentication.Length == 2
                        && ((IServiceDataSet)serviceDataSet).DataTables.Contains(this.Authentication[0])
                        && ((IServiceDataSet)serviceDataSet)[this.Authentication[0]].CommandText.Contains(this.Authentication[1]))//로그인 시도 이면
                    {
                        response = ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);

                        if (response.Status == Status.OK)//정상적으로 로그인이면 Token 추가
                        {

                            if (this.SessionMode == null || this.SessionMode == "" || this.SessionMode == "IIS")
                            {
                                if (this.DuplicateLogin)
                                {
                                    response.Message = Guid.NewGuid().ToString();
                                    this.Token.Add(response.Message, DateTime.MaxValue);
                                }
                                else
                                {
                                    tmp = (string)((IServiceDataSet)serviceDataSet)[Authentication[0]].GetValue("@EMAIL");

                                    //기존에 로그인 사용자가 있으면? 제거하고 신규 Guid로 할당
                                    if (this.Login.ContainsKey(tmp))
                                    {
                                        this.Token.Remove(this.Login[tmp]);//Token 제거

                                        response.Message = Guid.NewGuid().ToString();
                                        this.Token.Add(response.Message, DateTime.MaxValue);

                                        this.Login[tmp] = response.Message;
                                    }
                                    else
                                    {
                                        response.Message = Guid.NewGuid().ToString();
                                        this.Token.Add(response.Message, DateTime.MaxValue);

                                        this.Login.Add(tmp, response.Message);
                                    }
                                }
                            }
                            else//DB용 구현해야 함
                            {
                                response.Message = Guid.NewGuid().ToString();
                                this.Token.Add(response.Message, DateTime.MaxValue);
                            }
                        }

                        return (Response)response;
                    }

                    if (this.ByPassList.ContainsKey(((IServiceDataSet)serviceDataSet).DataTables[0].TableName))//인증없이 처리 가능(로그인,가입, 패스워드 변경,메시지 리스트 등)
                    {
                        if (((IServiceDataSet)serviceDataSet).DataTables.Contains("Token") && ((IServiceDataSet)serviceDataSet)["Token"].ConnectionName == "Atomus")
                            ((IServiceDataSet)serviceDataSet).DataTables.Remove("Token");

                        foreach (string value in this.ByPassList.Values)
                        {
                            if (((IServiceDataSet)serviceDataSet)[0].CommandText != null && ((IServiceDataSet)serviceDataSet)[0].CommandText.Contains(value))
                            {
                                response = ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);

                                //if (response.Status == Status.OK)
                                //    response.Message = "ByPassListTestToken";
                                //else
                                //    response.Message += "ByPassListTestToken";

                                return (Response)response;
                            }
                        }
                    }

                    //Token Command가 없거나
                    //ConnectionName이 Atomus 아니거나
                    //CommandText에 Token값이 없으면
                    //this.Token.Keys에 없는 토큰이면
                    if (!((IServiceDataSet)serviceDataSet).DataTables.Contains("Token")
                        || ((IServiceDataSet)serviceDataSet)["Token"].ConnectionName != "Atomus"
                        || ((IServiceDataSet)serviceDataSet)["Token"].CommandText == null
                        || ((IServiceDataSet)serviceDataSet)["Token"].CommandText == ""
                        || !this.Token.Keys.Contains(((IServiceDataSet)serviceDataSet)["Token"].CommandText))
                    {
                        response = (Response)Factory.CreateInstance("Atomus.Service.Response", false, true);
                        response.Status = Status.Failed;
                        response.Message = this.UnauthenticatedUserMessage;

                        return (Response)response;
                    }

                    if (this.Token[((IServiceDataSet)serviceDataSet)["Token"].CommandText] < DateTime.Now)
                    {
                        response = (Response)Factory.CreateInstance("Atomus.Service.Response", false, true);
                        response.Status = Status.Failed;
                        response.Message = this.ExpiredCertificateMessage;

                        return (Response)response;
                    }

                    //if (this.Token[((IServiceDataSet)serviceDataSet)["Token"].CommandText] != DateTime.MaxValue)
                    //    this.Token[((IServiceDataSet)serviceDataSet)["Token"].CommandText] = DateTime.Now.AddMinutes(20);

                    ((IServiceDataSet)serviceDataSet).DataTables.Remove("Token");

                    return ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);
                }
            }
            catch (AtomusException exception)
            {
                DiagnosticsTool.MyTrace(exception);

                response = ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);
                response.ExceptionInfomation = new ExceptionInfomation(exception);
                //response.Message += tmp;
                return (Response)response;

                //return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);

                response = ((IService)Factory.CreateInstance(serviceDataSet.ServiceName)).Request(serviceDataSet);
                response.ExceptionInfomation = new ExceptionInfomation(exception);
                //response.Message += tmp;
                return (Response)response;

                //return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }
            finally
            {
                if (this.log != null)
                    if (logData != null)
                    {
                        logData["REQUEST_RESULT"] = response == null ? null : response.Message;
                        logData["END_DATETIME"] = DateTime.Now;

                        this.log.ControlAction(this, "", logData);
                    }
            }
        }

        private string GetClientIPAddress()
        {
            try
            {
                if (OperationContext.Current == null)
                    return "";
                else
                    return "";
                //return (OperationContext.Current.IncomingMessageProperties[System.ServiceModel.Channels.RemoteEndpointMessageProperty.Name] as System.ServiceModel.Channels.RemoteEndpointMessageProperty).Address;
            }
            catch (Exception)
            {
                return "";
            }
        }


        private void ReadServiceDataSet(IServiceDataSet serviceDataSet, System.Text.StringBuilder stringBuilder)
        {
            switch (serviceDataSet.ServiceName)
            {
                case "Atomus.Service.DefaultService":
                    this.ReadServiceDataSetDefaultService(serviceDataSet, stringBuilder);
                    break;

                default:
                    stringBuilder.Append(serviceDataSet.Count > 0 ? string.Format("{0}\n{1}", serviceDataSet[0].DataTable.DataSet.GetXmlSchema(), serviceDataSet[0].DataTable.DataSet.GetXml()) : null);
                    break;

            }
        }
        private void ReadServiceDataSetDefaultService(IServiceDataSet serviceDataSet, System.Text.StringBuilder stringBuilder)
        {
            try
            {
                stringBuilder.AppendLine($"{serviceDataSet.ServiceName}\t{serviceDataSet.TransactionScope}");

                foreach (System.Data.DataTable table in (serviceDataSet).DataTables)
                {
                    stringBuilder.AppendLine($"{serviceDataSet[table.TableName].CommandType}\t{serviceDataSet[table.TableName].ConnectionName}\t{serviceDataSet[table.TableName].CommandText}");

                    foreach (System.Data.DataColumn dataColumn in table.Columns)
                    {
                        stringBuilder.Append($"{serviceDataSet[table.TableName].GetAttribute(dataColumn.ColumnName, "DbType")}\t{serviceDataSet[table.TableName].GetAttribute(dataColumn.ColumnName, "Size")}\t");
                        if (serviceDataSet[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetTableName") != null)
                            stringBuilder.Append($"{table.TableName}\t{dataColumn.ColumnName}\t{serviceDataSet[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetTableName")}\t{serviceDataSet[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetParameterName")}");

                        stringBuilder.AppendLine("");
                    }

                    foreach (System.Data.DataRow dataRow in table.Rows)
                        foreach (System.Data.DataColumn dataColumn in table.Columns)
                            stringBuilder.AppendLine($"{dataColumn.ColumnName}\t{dataRow[dataColumn.ColumnName]}");
                }
            }
            catch (Exception ex)
            {
                stringBuilder.AppendLine(ex.ToString());
            }
        }

        async Task<Response> IServiceAsync.RequestAsync(ServiceDataSet serviceDataSet)
        {
            var t = Task.Run(() =>
                (this as IService).Request(serviceDataSet)
            );

            return await t;
        }
    }
}
