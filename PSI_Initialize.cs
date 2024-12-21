using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using HaierSFADEV.HttpTrigger.Budget;
using HaierSFADEV.Utilites;
using Microsoft.PowerPlatform.Dataverse.Client;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using System.Linq;
using Microsoft.PowerPlatform.Dataverse.Client.Extensions;
using System.Data;
using NPOI.OpenXml4Net.OPC;
using NPOI.HSSF.Record;
using NPOI.SS.Formula.Functions;


namespace HaierSFA.HttpTrigger.RollingBudget_PSI
{
    /// <summary>
    /// PSI��������
    /// </summary>
    public static class PSI_Initialize
    {
        [FunctionName("PSI_Initialize")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            try
            {
                // ��������ͻ���
                using ServiceClient serviceClient = new ServiceClient(CDSHelper.GetDataverseConnectionString());
                if (!serviceClient.IsReady)
                {
                    log.LogError($"���������ڲ����󡿷�������������");
                    return new BadRequestObjectResult(new VersionControlResponse
                    {
                        Status = StatusCodes.Status500InternalServerError.ToString(),
                        Message = "���`�Щ`�ڲ�����`���k�����ޤ�����"
                    });
                }

                #region ��ǰ�˻�ȡ��Ϣ
                //��ȡ�����������Ϣ
                var Rolling_Detail = data?.detail ?? null;
                //��ȡ��¼����
                var mainRollingBudgetId = Rolling_Detail[0]?.applicationNumber ?? string.Empty;
                //��ȡ�ͷ�
                var psi_allModels = data?.Models ?? string.Empty;
                //��ȡ���
                int Year = Convert.ToInt32(Rolling_Detail[0].year ?? 0);
                //�·�
                int[] month = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
                //��ȡ�汾
                string round = Rolling_Detail[0]?.versionNum ?? string.Empty;
                //��ȡ��ǰ��VersionNumber
                string versionNumber = Year.ToString() + round.Substring(0, 2);

                //��ȡ��һ��Ŀ��VersionNumber
                int versionAsNumber;
                string lastVersionNumber;
                try
                {
                    versionAsNumber = int.Parse(versionNumber) - 1;
                    lastVersionNumber = versionAsNumber.ToString();
                }
                catch (Exception ex)
                {
                    log.LogError($"���������������{ex.Message}");
                    return new BadRequestObjectResult(new VersionControlResponse
                    {
                        Status = StatusCodes.Status400BadRequest.ToString(),
                        Message = "���ťЩ`������g�`�äƤ��ޤ���"// ����İ汾����
                    });
                }
                //�洢ǰ�˴�����ͷ�
                List<string> psi_allModelsList = new List<string>();
                foreach (var entity in psi_allModels)
                {

                    psi_allModelsList.Add(entity.Value);
                }


                #endregion

                #region ��ѯXml
                // ���ͷ���Ϊ��ѯ������ת��ΪFetchXML��ѯ��Ʒ��
                string psi_allModelsCode_fetch = $"<condition attribute='sfa_name' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>") )}</condition>";
                // ���ͷ���Ϊ��ѯ������ת��ΪFetchXML��ѯ����Ԥ����ϸ
                string psi_allModelsCode_fetch_RollingDeatil = $"<condition attribute='sfa_p_name' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                // ���ͷ���Ϊ��ѯ������ת��ΪFetchXML��ѯԤ����ϸ
                string psi_allModelsCode_fetch_BudgetDeatil = $"<condition attribute='sfa_modelname' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                // ���ͷ���Ϊ��ѯ������ת��ΪFetchXML��ѯPSI��ϸ
                string psi_allModelsCode_AdjustDeatil_fetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                //����¼������Ϊ��ѯPSI��ϸ�������
                string psi_applicationNumber_fetchxml = $"<condition attribute='sfa_title' operator='eq' value='{mainRollingBudgetId}'/>";
                //��ѯ12���µ�xml
                string psi_Month_fetch = string.Join("\r\n", Array.ConvertAll(month, item => $"<condition attribute='sfa_month' operator='eq' value='{item}'/>"));
                //��ѯ��ǰ��¼���ŵ�XML
                string mainRollingIdXml = $"<condition attribute='sfa_sn' operator='eq' value='{mainRollingBudgetId}'/>";
                string main_PSI_IdXml = $"<condition attribute='sfa_title' operator='eq' value='{mainRollingBudgetId}'/>";
                //��ѯ��ǰ��Ⱥͻ�Ŀ��XML
                string versionNumberXml = $"<condition attribute='sfa_versionnumber' operator='eq' value='{versionNumber}'/>";
                //��ѯ��ǰ�����һ��Ŀ��XML
                string lastVersionNumberXml = $"<condition attribute='sfa_versionnumber' operator='eq' value='{lastVersionNumber}'/>";
                //��ѯ��ǰ���Xml
                string yearXml = $"<condition attribute='sfa_year' operator='eq' value='{Year}'/>";
                //��ѯȥ���Xml
                string lastYearXml = $"<condition attribute='sfa_year' operator='eq' value='{Year - 1}'/>";
                #endregion

                //����Ʒ���л�ȡ��ȫ���ͷ���SAPcode��Keycode
                EntityCollection psi_allModels_keycode = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModels", psi_allModelsCode_fetch), serviceClient);

                //�ӷ���master���л�ȡ������PSI����ΪTrue�ķ���
                EntityCollection entity_Psi_LegalCollection = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Legal_Master("BR_PSI_Legal"), serviceClient);

                //�洢PSI����ΪTrue��name��code
                List<string> sfa_legal_codeList = new List<string>();
                List<string> sfa_legal_nameList = new List<string>();
                foreach (var entity in entity_Psi_LegalCollection.Entities)
                {
                    if (entity.Contains("sfa_name") && entity["sfa_name"] != null)
                    {
                        sfa_legal_nameList.Add(entity["sfa_name"].ToString());
                    }
                    if (entity.Contains("sfa_sapcode") && entity["sfa_sapcode"] != null)
                    {
                        sfa_legal_codeList.Add(entity["sfa_sapcode"].ToString());
                    }
                }

                //��PSIΪtrue�ķ�����Ϊ��ѯ�������������ת��ΪFetchXML
                string sfa_fetch_psi_legalName = $"<condition attribute='sfa_c_name' operator='in'>{string.Join("", sfa_legal_nameList.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                //��PSIΪtrue�ķ�����Ϊ��ѯ������������ת��ΪFetchXML
                string sfa_fetch_legalName = string.Join("\r\n", sfa_legal_nameList.ConvertAll(item => $"<condition attribute='sfa_name' operator='eq' value='{item}' />"));
                //��PSIΪtrue�ķ���code��Ϊ��ѯʵ��������������ת��ΪFetchXML
                string sfa_fetch_psi_legalcode_xml = string.Join("\r\n", sfa_legal_codeList.ConvertAll(item => $"<condition attribute='sfa_c_sapcode' operator='eq' value='{item}' />"));
                //�ӹ���������л�ȡ�ͷ����ͷ���Ӧ�ķ����Լ���Ӧ��SAPcode
                EntityCollection psi_ModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumberAndLegal", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml), serviceClient);
                //�ӵ��������л�ȡ�ͷ���PSI�����˵����
                EntityCollection ModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI("B001", yearXml, sfa_fetch_legalName, psi_allModelsCode_fetch), serviceClient);
                //�洢PSI�ͷ����ͷ���Ӧ�ķ���
                var psi_all_ModelsAndlegal = ModelsAndLegal.Entities
     .Where(entity => entity.Contains("sfa_kunnr") && entity.Contains("sfa_zcusmodel"))
     .Select(entity => new
     {
         sfa_kunnr = entity.Contains("sfa_kunnr") ? ((EntityReference)entity["sfa_kunnr"]).Name : null,  // ��ȡ sfa_kunnr �� Name
         sfa_kunnr_code = entity.Contains("EMP1.sfa_sapcode") ? entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode")?.Value : null,  // ��ȫ���� EMP1.sfa_sapcode
         sfa_zcusmodel = entity.Contains("sfa_zcusmodel") ? ((EntityReference)entity["sfa_zcusmodel"]).Name : null,  // ��ȡ sfa_zcusmodel �� Name
         sfa_zcusmodel_code = entity.Contains("EMP2.sfa_sapcode") ? entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode")?.Value : null  // ��ȫ���� EMP2.sfa_sapcode
     })
     .ToList();


                //��ȡ�ͷ���SAPcode���洢��List������
                List<string> sfa_p_code_list = new List<string>();
                foreach (var entity in ModelsAndLegal.Entities)
                {
                    if (entity.Contains("EMP1.sfa_sapcode") && entity["EMP1.sfa_sapcode"] != null)
                    {
                        sfa_p_code_list.Add(entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode").Value.ToString());
                    }
                }

                //��ȡ���˵�SAPcode���洢��List������
                List<string> sfa_c_code_list = new List<string>();
                foreach (var entity in ModelsAndLegal.Entities)
                {
                    if (entity.Contains("EMP2.sfa_sapcode") && entity["EMP2.sfa_sapcode"] != null)
                    {
                        sfa_c_code_list.Add(entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode").Value.ToString());
                    }
                }

                //���ͷ���SAPcode��Ϊ��ѯAI���Ĵ洢����ת��ΪFetchXML
                string sfa_fetch_p_codeXml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("", sfa_p_code_list.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                //�����˵�SAPcode��Ϊ��ѯAI���Ĵ洢����ת��ΪFetchXML
                string sfa_fetch_c_codeXml = $"<condition attribute='sfa_c_sapcode' operator='in'>{string.Join("", sfa_c_code_list.ConvertAll(item => $"<value>{item}</value>"))}</condition>";

                // �� Dataverse ��ṹ���� DataTable
                string entityLogicalName = "sfa_psi_details"; // �滻ΪĿ����߼�����
                DataTable dt = CreateDataTableFromDataverseSchema(entityLogicalName, serviceClient);
                //���ݵ�¼�����ж�PSI��ϸ�����Ƿ���ڶ�Ӧ�ļ�¼
                EntityCollection entityCollection_record = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_PSI_Deatils", main_PSI_IdXml, "")));
                if (entityCollection_record.Entities.Count == 0)
                {
                    #region ��������
                    //���ݵ�¼���ţ��ͷ����·ݴ�PSIԔ�����в�ѯ
                    EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", psi_allModelsCode_AdjustDeatil_fetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                    //�洢12���¶�Ӧ�ͷ��ĵ���������List
                    List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                    foreach (var model in psi_allModelsList)
                    {
                        for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                        {
                            bool flag = true;
                            foreach (var adjust_record in psi_adjust_num.Entities)
                            {
                                //�Ƚϼ�¼�е��ͷ����·ݣ��Լ��Ƿ����sfa_adjusted_quantities����
                                if (adjust_record.Contains("sfa_p_names") && model == adjust_record.Contains("sfa_p_names").ToString() && adjust_record.Contains("sfa_adjusted_quantities") && adjust_record.Contains("sfa_month") && Convert.ToInt32(adjust_record.Contains("sfa_month").ToString()) == adjust_month)
                                {
                                    all_month_AdjustNum.Add(new ModelMonthNum
                                    {
                                        Model = adjust_record["sfa_p_names"].ToString(),
                                        Month = adjust_month,
                                        ModelSum = Convert.ToDecimal(adjust_record["sfa_adjusted_quantities"])
                                    });
                                    flag = false;
                                    break;
                                }
                            }
                            if (flag)
                                all_month_AdjustNum.Add(new ModelMonthNum
                                {
                                    Model = model,
                                    Month = adjust_month,
                                    ModelSum = 0
                                });
                        }
                    }
                    #endregion

                    #region ��غ�Ӌ,������
                    //����versionNumber��PSI���˻�ȡ��ǰ��Ŀʮ�����·ݴ����ͷ��ļ�¼
                    EntityCollection psi_Jan_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml)));
                    //�洢ÿ���ͷ�12���µ�����
                    List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                    //�洢ÿ���ͷ�12���µĥ�����
                    List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                    //����ǰ�˴����ͷ��ͷ��˵�ȫ�����
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_monthNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                            });
                                            all_ModelReasonList.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                Reason = validEntity.Contains("sfa_num_reason") ? validEntity["sfa_num"].ToString() : ""
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                    all_ModelReasonList.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        Reason = ""
                                    });
                                }
                            }
                            else
                            {
                                all_monthNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                                all_ModelReasonList.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    Reason = ""
                                });
                            }
                        }
                    }
                    //�����ͷ����·ݽ�����������
                    var round_Number = all_monthNum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    var all_reason = all_ModelReasonList
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                })
                .ToList();
                    #endregion

                    #region ǰ�غ�Ӌ
                    //����versionNumber��PSI���˻�ȡ��һ��Ŀʮ�����·ݴ����ͷ��ļ�¼
                    EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                    //�洢��һ��Ŀÿ���ͷ�12���µ�����
                    List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                    //����ǰ�˴����ͷ��ͷ��˵�ȫ�����
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_last_monthNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_last_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_last_monthNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    var lastRound_Number = all_last_monthNum
               .GroupBy(x => new { x.Model, x.Month })
               .Select(g => new ModelMonthNum
               {
                   Model = g.Key.Model,
                   Month = g.Key.Month,
                   ModelSum = g.Sum(x => x.ModelSum)
               })
               .ToList();
                    #endregion

                    #region ����
                    //��ȡ�������°汾
                    EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                    //�洢ÿ���ͷ�12���µ���������
                    List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                    //����-�汾��
                    string versionnumber = "";
                    if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                    {
                        var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                        //����-�汾��
                        versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                    }
                    else
                    {
                        return new BadRequestObjectResult(new VersionControlResponse
                        {
                            Status = StatusCodes.Status500InternalServerError.ToString(),
                            Message = "�x�k�������ˤˤ��Є��ͷ������ڤ��Ƥ��ޤ��󡢌�������_�J���Ƥ�������"
                        });
                    }

                    //��PSIΪtrue�ķ�����Ϊ��ѯ������ϸ�Ĵ洢����ת��ΪFetchXML
                    string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                    //��Ѱ��ǰ�汾num��xml
                    string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                    //��ȡ������ϸ
                    EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (result_Budget_BVersion.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = result_Budget_BVersion.Entities
                .Where(entity =>
                    entity.Contains("sfa_modelname") &&
                    entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_legalname") &&
                    entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_budget_monthNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_modelname"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_budget_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_budget_monthNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    //�����ͷ����·ݽ������������
                    var eachModel_BugetNumber = all_budget_monthNum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    #endregion

                    #region ֱ��ǰ��
                    //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                    List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_direct_beforeNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_direct_beforeNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_direct_beforeNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    var round_direct_beforeNum = all_direct_beforeNum
               .GroupBy(x => new { x.Model, x.Month })
               .Select(g => new ModelMonthNum
               {
                   Model = g.Key.Model,
                   Month = g.Key.Month,
                   ModelSum = g.Sum(x => x.ModelSum)
               })
               .ToList();
                    #endregion

                    #region ֱ�ͺ��
                    //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                    List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_direct_afterNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_direct_afterNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_direct_afterNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }

                    var round_direct_afterNum = all_direct_afterNum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    #endregion

                    #region ǰ��ֱ��ǰ��
                    //�洢��һ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                    List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_last_direct_before_monthNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_last_direct_before_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_last_direct_before_monthNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    //�����ͷ����·ݽ�ֱ��ǰ�����
                    var lastRound_direct_before_Number = all_last_direct_before_monthNum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    #endregion

                    #region ǰ��ֱ�ͺ��
                    //�洢��һ��Ŀÿ���ͷ�12����ֱ�ͺ�������
                    List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                    //�����ͷ�����

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //�ȽϹ���������д��ڵļ�¼
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ��ͷ������
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_last_direct_after_monthNum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_last_direct_after_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_last_direct_after_monthNum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    //�����ͷ����·ݽ�ֱ�ͺ�����
                    var lastRound_direct_after_Number = all_last_direct_after_monthNum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    #endregion

                    #region AI��y
                    EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                    //�洢ÿ���ͷ�12���µ�AI��������
                    List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m����12���·�
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;

                            if (psi_AINumber.Entities.Count != 0)
                            {
                                //��ȡ��Ч���ͷ�code�ͷ���code���
                                var validCombination = psi_AINumber.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_sapcode") &&
                    entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // ƥ�� sfa_zcusmodel_code
                    entity.Contains("sfa_c_sapcode") &&
                    entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // ƥ�� sfa_kunnr_code
                .ToList();
                                //����з��Ͻ��ж�Ӧ��¼�����
                                if (validCombination.Any())
                                {
                                    // ������Ч����ϣ����к�������
                                    foreach (var validEntity in validCombination)
                                    {
                                        if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                        {
                                            all_monthAINum.Add(new ModelMonthNum
                                            {
                                                Model = validEntity["sfa_p_name"].ToString(),
                                                Month = m,
                                                ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                            });
                                            flag = true;
                                        }
                                    }
                                    //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                }
                                if (!flag)
                                {
                                    all_monthAINum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                            else
                            {
                                all_monthAINum.Add(new ModelMonthNum
                                {
                                    Model = model_legal.sfa_zcusmodel,
                                    Month = m,
                                    ModelSum = 0
                                });
                            }
                        }
                    }
                    //�����ͷ����·ݽ�AI�������
                    var round_AINumber = all_monthAINum
                .GroupBy(x => new { x.Model, x.Month })
                .Select(g => new ModelMonthNum
                {
                    Model = g.Key.Model,
                    Month = g.Key.Month,
                    ModelSum = g.Sum(x => x.ModelSum)
                })
                .ToList();
                    #endregion

                    #region �g��,�g��,������
                    //�����Ŵ���������б�
                    List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                    // ����һ����Ōg�ӽ�����б�
                    List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                    // ����һ����Ōg��������б�
                    List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                    //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ����όg���Ʃ`�֥���ж�Ӧ�ļ�¼
                    EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                    //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ�������ж�Ӧ�ļ�¼
                    EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                    // ���·ݺ�sap_p_code���з���
                    var achi_monthlySums = psi_achi_number.Entities
                        .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                        .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                        .ToDictionary(
                            group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                            group => new
                            {
                                QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // ����sfa_grossprofit�ܺ�
                            }
                        );
                    // ���·ݺ�sap_p_code���з���
                    var des_monthlySums = psi_Des_number.Entities
                        .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                        .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                        .ToDictionary(
                            group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                            group => new
                            {
                                QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // ����sfa_grossprofit�ܺ�
                            }
                        );



                    // ���ͷ�codeListת��ΪHashSetList
                    HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                    foreach (var scode in sfa_p_code_hashSet)
                    {
                        // ����g����ë�����g�ӵ�ֵ
                        for (int m = 1; m <= 12; m++)
                        {
                            // ��ȡachi��des�ж�Ӧ�·ݵ�����
                            string p_code = "";
                            decimal achiQuantity = 0;
                            decimal desQuantity = 0;
                            decimal achiGrossProfit = 0, desGrossProfit = 0;
                            p_code = scode;
                            // ���achi_monthlySums�д��ڸ��·ݵ����ݣ����ȡ������ë��
                            if (achi_monthlySums.ContainsKey((m, scode)))
                            {
                                achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                            }

                            // ���des_monthlySums�д��ڸ��·ݵ����ݣ����ȡ����
                            if (des_monthlySums.ContainsKey((m, scode)))
                            {
                                desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                            }
                            // ���������
                            decimal totalQuantity = achiQuantity + desQuantity;
                            //��ë�����
                            decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                            //�g��ֵ
                            decimal totalAchiQuantity = achiQuantity;
                            // ����һ��ModelMonthNum���󲢽���������б�
                            achi_des_num.Add(new ModelMonthNum
                            {
                                Model = p_code,
                                Month = m,
                                ModelSum = totalQuantity
                            });
                            achi_num.Add(new ModelMonthNum
                            {
                                Model = p_code,
                                Month = m,
                                ModelSum = totalAchiQuantity
                            });
                            grossProfit_num.Add(new ModelMonthNum
                            {
                                Model = p_code,
                                Month = m,
                                ModelSum = totalGrossProfit
                            });
                        }

                    }
                    #endregion

                    #region ͬ��

                    //������ȡ���ͷ���KeyCode����
                    HashSet<string> models_kecode_list = new HashSet<string>();
                    List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                    foreach (var kecode_record in psi_allModels_keycode.Entities)
                    {
                        if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                        {
                            models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                        }
                    }

                    //���ݻ�ȡ����KeyCodeƴ��ΪXML������KeyCodeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                    string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                    //����KeycdoeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                    EntityCollection psi_keycode_allLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModels", model_kecode_fetchxml), serviceClient);

                    for (int i = 1; i <= 12; i++)
                    {
                        foreach (var record in psi_keycode_allLegal.Entities)
                        {
                            if (record.Contains("sfa_sapcode") && record.Contains("sfa_keycode"))
                            {
                                models_kecode.Add(new ModelMonthNum
                                {
                                    Model = record["sfa_sapcode"].ToString(),
                                    Month = i,
                                    Keycode = record["sfa_keycode"].ToString(),
                                    ModelSum = 0
                                });
                            }
                        }
                    }

                    //�� psi_keycode_allModels����ȡ�ͷ���sfa_sapcode��ƴ�ӳɲ�ѯʵ����������е�XML ����
                    string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("",
     psi_keycode_allLegal.Entities
         .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // ���˰��� sfa_sapcode ���Ե�ʵ��
         .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>")
 )}</condition>";

                    //����allModel_kecode_fetchxml��sfa_fetch_psi_legalcode_xml��ʵ�����������в�ѯȥ���Ӧ������
                    EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                    EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                    // ����SapCode���·ݷ��鲢����ʵ�������������ܺ�
                    var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                        .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                        .GroupBy(entity => new
                        {
                            sapcode = (string)entity["sfa_p_sapcode"],
                            Month = (int)entity["sfa_month"]
                        }) // �� Keycode ���·ݷ���
                        .ToDictionary(
                            group => group.Key, // ����ļ�Ϊ Keycode �� Month
                            group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                        );
                    var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                        .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                        .GroupBy(entity => new
                        {
                            sapcode = (string)entity["sfa_p_sapcode"],
                            Month = (int)entity["sfa_month"]
                        }) // �� Keycode ���·ݷ���
                        .ToDictionary(
                            group => group.Key, // ����ļ�Ϊ Keycode �� Month
                            group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                        );

                    //��achiGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                    foreach (var model in models_kecode)
                    {
                        var key = new
                        {
                            sapcode = model.Model, // Model ��Ӧ sapcode
                            Month = model.Month
                        };

                        if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                        {
                            model.ModelSum = value; // ��ֵ��Ӧ���ܺ�
                        }
                    }

                    //��desGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                    foreach (var model in models_kecode)
                    {
                        var key = new
                        {
                            sapcode = model.Model, // Model ��Ӧ sapcode
                            Month = model.Month
                        };

                        if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                        {
                            model.ModelSum += value; // ��ֵ���
                        }
                    }
                    var mergedModelsKecode = models_kecode
        .GroupBy(item => new { item.Keycode, item.Month }) // �� Keycode �� Month ����
        .Select(group => new ModelMonthNum
        {
            Keycode = group.Key.Keycode,
            Month = group.Key.Month,
            Model = string.Join(";", group.Select(x => x.Model)), // �ϲ� Model ֵ������Ҫ��
            ModelSum = group.Sum(x => x.ModelSum) // ���� ModelSum
        })
        .ToList();

                    #endregion

                    //���������
                    foreach (var item in round_Number)
                    {
                        // ����һ������
                        DataRow newRow = dt.NewRow();
                        #region ��غϼ�
                        // Ϊ sfa_month �и�ֵ���·�
                        newRow["sfa_month"] = item.Month; // ��ֵ�·�

                        // Ϊ sfa_p_names �и�ֵ��Model
                        newRow["sfa_p_names"] = item.Model; // ��ֵ Model

                        // Ϊ sfa_all_sum_number �и�ֵ��ModelSum������ round_Number��
                        newRow["sfa_all_sum_number"] = item.ModelSum; // ��ֵ round_Number �� ModelSum
                        #endregion
                        //�����ͷ���item.Model��ֵ��psi_ModelsAndLegal��sfa_p_code��ֵ����ƥ��
                        foreach (var pname in psi_ModelsAndLegal.Entities)
                        {
                            if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                            {
                                newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // ��ֵ direct_beforeNumber �� ModelSum
                                newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // ��ֵ direct_beforeNumber �� ModelSum
                                break;
                            }

                        }
                        #region ǰ�غϼ�
                        // ���Ҷ�Ӧ�� lastRound_Number ����
                        var lastRound = lastRound_Number
                                        .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                        if (lastRound != null)
                        {
                            newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            newRow["sfa_allsumnumber_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ǰ�ز�
                        // ����ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                        var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                        // Ϊ sfa_num_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                        newRow["sfa_num_balance"] = previousDiff;
                        #endregion
                        #region ����
                        // ���Ҷ�Ӧ��Ԥ������
                        var budgetDetail = eachModel_BugetNumber
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                        if (budgetDetail != null)
                        {
                            newRow["sfa_budget_num"] = budgetDetail.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_budget_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ͬ��
                        // ���Ҷ�Ӧ��ͬ������
                        var samePeriod_num = mergedModelsKecode
                                            .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                        // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                        if (samePeriod_num != null)
                        {
                            newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_sameperiod_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ֱ��ǰ��
                        // ���Ҷ�Ӧ��ֱ��ǰ������
                        var direct_beforeNumber = round_direct_beforeNum
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                        if (direct_beforeNumber != null)
                        {
                            newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_direct_before"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ֱ�ͺ��
                        // ���Ҷ�Ӧ��ֱ�ͺ������
                        var direct_afterNumber = round_direct_afterNum
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                        if (direct_afterNumber != null)
                        {
                            newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_direct_after"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ǰ��ֱ��ǰ��
                        // ���Ҷ�Ӧ��ֱ��ǰ������
                        var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                        if (lastRound_direct_beforeNumber != null)
                        {
                            newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_direct_before_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ǰ��ֱ�����
                        // ���Ҷ�Ӧ��ֱ�ͺ������
                        var last_direct_afterNumber = lastRound_direct_after_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                        if (last_direct_afterNumber != null)
                        {
                            newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_direct_after_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ֱ��ǰ���
                        //����ֱ��ǰ���(ֱ��ǰ��-ǰ��ֱ��ǰ��)
                        var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                        // Ϊ sfa_direct_before_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                        newRow["sfa_direct_before_balance"] = before_half_dif;
                        #endregion
                        #region ֱ������
                        //����ֱ������(ֱ�����-ǰ��ֱ�����)
                        var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                        // Ϊ sfa_direct_after_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                        newRow["sfa_direct_after_balance"] = after_half_dif;
                        #endregion
                        #region AI��y
                        // ���Ҷ�Ӧ��AI�������
                        var AI_number = round_AINumber
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                        if (AI_number != null)
                        {
                            newRow["sfa_ai_num"] = AI_number.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_ai_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region �g��
                        var achi_des_record = achi_des_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                        if (achi_des_record != null)
                        {
                            newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            newRow["sfa_sales_perf_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region �g��
                        var achi_record = achi_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                        if (achi_record != null)
                        {
                            newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            newRow["sfa_sales_retail_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ������
                        var gro_record = grossProfit_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                        if (gro_record != null && achi_des_record.ModelSum != 0)
                        {
                            newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            newRow["sfa_perf_grossprofit"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region �M����
                        //�g��/ǰ�غ�Ӌ
                        if (achi_des_record != null && lastRound.ModelSum != 0)
                        {
                            newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                        }
                        else
                        {
                            newRow["sfa_progressrate"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ������
                        var model_reason = all_ModelReasonList
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                        if (model_reason != null)
                        {
                            newRow["sfa_num_reason"] = model_reason.Reason; // ��ֵ direct_beforeNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ���������ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_num_reason"] = ""; // ���û���ҵ���Ӧ���ݣ���ֵΪ "" ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ��������
                        var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                        if (Adjust_num != null)
                        {
                            newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                        }
                        else
                        {
                            // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                            newRow["sfa_adjusted_quantities"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                        }
                        #endregion
                        #region ���ɱ���

                        #endregion
                        // ��������ӵ� DataTable
                        dt.Rows.Add(newRow);
                    }
                }
                else
                {
                    //PSI��ϸ���о��и��ͷ���¼��ֱ����ȡ���ݣ����û�����ifͬ�߼�ȥȡ��Ӧ�ͷ���Ӧ������
                    var psiDetails_ModelsAndlegal = entityCollection_record.Entities
    .Where(entity => entity.Contains("sfa_title") && entity.Contains("sfa_p_names"))
    .Select(entity => new
    {
        sfa_kunnr = "PSI",
        sfa_zcusmodel = entity["sfa_p_names"].ToString()  // ��ȡ sfa_zcusmodel �� Name
    })
    .Distinct()
    .ToList();
                    #region û����ǰ��ƥ����ͷ�
                    // ����һ�����ϴ洢 psiDetails_ModelsAndlegal �е� sfa_zcusmodel ֵ
                    var psiDetailsModelsSet = new HashSet<string>(
                        psiDetails_ModelsAndlegal.Select(item => item.sfa_zcusmodel)
                    );

                    // ��ȡ psi_allModelsList ���� psiDetailsModelsSet ��ͬ����
                    var commonItems = psi_allModelsList
                        .Where(model => psiDetailsModelsSet.Contains(model))
                        .Distinct()
                        .ToList();
                    // ��ȡ psi_allModelsList ���� psiDetailsModelsSet ��ͬ����
                    var differentItems = psi_allModelsList
                        .Where(model => !psiDetailsModelsSet.Contains(model))
                        .ToList();
                    //û����ͬ��������Ӹ������л�ȡ����
                    if (commonItems.Count == 0)
                    {
                        #region ��������
                        //���ݵ�¼���ţ��ͷ����·ݴ�PSIԔ�����в�ѯ
                        EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", psi_allModelsCode_AdjustDeatil_fetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                        //�洢12���¶�Ӧ�ͷ��ĵ���������List
                        List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                        foreach (var model in psi_allModelsList)
                        {
                            for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                            {
                                bool flag = true;
                                foreach (var adjust_record in psi_adjust_num.Entities)
                                {
                                    //�Ƚϼ�¼�е��ͷ����·ݣ��Լ��Ƿ����sfa_adjusted_quantities����
                                    if (adjust_record.Contains("sfa_p_names") && model == adjust_record.Contains("sfa_p_names").ToString() && adjust_record.Contains("sfa_adjusted_quantities") && adjust_record.Contains("sfa_month") && Convert.ToInt32(adjust_record.Contains("sfa_month").ToString()) == adjust_month)
                                    {
                                        all_month_AdjustNum.Add(new ModelMonthNum
                                        {
                                            Model = adjust_record["sfa_p_names"].ToString(),
                                            Month = adjust_month,
                                            ModelSum = Convert.ToDecimal(adjust_record["sfa_adjusted_quantities"])
                                        });
                                        flag = false;
                                        break;
                                    }
                                }
                                if (flag)
                                    all_month_AdjustNum.Add(new ModelMonthNum
                                    {
                                        Model = model,
                                        Month = adjust_month,
                                        ModelSum = 0
                                    });
                            }
                        }
                        #endregion

                        #region ��غ�Ӌ,������
                        //����versionNumber��PSI���˻�ȡ��ǰ��Ŀʮ�����·ݴ����ͷ��ļ�¼
                        EntityCollection psi_Jan_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml)));
                        //�洢ÿ���ͷ�12���µ�����
                        List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                        //�洢ÿ���ͷ�12���µĥ�����
                        List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                        //����ǰ�˴����ͷ��ͷ��˵�ȫ�����
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                                });
                                                all_ModelReasonList.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    Reason = validEntity.Contains("sfa_num_reason") ? validEntity["sfa_num"].ToString() : ""
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                        all_ModelReasonList.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            Reason = ""
                                        });
                                    }
                                }
                                else
                                {
                                    all_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                    all_ModelReasonList.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        Reason = ""
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�����������
                        var round_Number = all_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        var all_reason = all_ModelReasonList
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                    })
                    .ToList();
                        #endregion

                        #region ǰ�غ�Ӌ
                        //����versionNumber��PSI���˻�ȡ��һ��Ŀʮ�����·ݴ����ͷ��ļ�¼
                        EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                        //�洢��һ��Ŀÿ���ͷ�12���µ�����
                        List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                        //����ǰ�˴����ͷ��ͷ��˵�ȫ�����
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        var lastRound_Number = all_last_monthNum
                   .GroupBy(x => new { x.Model, x.Month })
                   .Select(g => new ModelMonthNum
                   {
                       Model = g.Key.Model,
                       Month = g.Key.Month,
                       ModelSum = g.Sum(x => x.ModelSum)
                   })
                   .ToList();
                        #endregion

                        #region ����
                        //��ȡ�������°汾
                        EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                        //�洢ÿ���ͷ�12���µ���������
                        List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                        //����-�汾��
                        string versionnumber = "";
                        if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                        {
                            var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                            //����-�汾��
                            versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                        }
                        else
                        {
                            return new BadRequestObjectResult(new VersionControlResponse
                            {
                                Status = StatusCodes.Status500InternalServerError.ToString(),
                                Message = "�x�k�������ˤˤ��Є��ͷ������ڤ��Ƥ��ޤ��󡢌�������_�J���Ƥ�������"
                            });
                        }

                        //��PSIΪtrue�ķ�����Ϊ��ѯ������ϸ�Ĵ洢����ת��ΪFetchXML
                        string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.Select(item => $"<value>{item}</value>")
)}</condition>";
                        //��Ѱ��ǰ�汾num��xml
                        string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                        //��ȡ������ϸ
                        EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (result_Budget_BVersion.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = result_Budget_BVersion.Entities
                    .Where(entity =>
                        entity.Contains("sfa_modelname") &&
                        entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_legalname") &&
                        entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_budget_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_modelname"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_budget_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_budget_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ������������
                        var eachModel_BugetNumber = all_budget_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ֱ��ǰ��
                        //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_direct_beforeNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_direct_beforeNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_direct_beforeNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        var round_direct_beforeNum = all_direct_beforeNum
                   .GroupBy(x => new { x.Model, x.Month })
                   .Select(g => new ModelMonthNum
                   {
                       Model = g.Key.Model,
                       Month = g.Key.Month,
                       ModelSum = g.Sum(x => x.ModelSum)
                   })
                   .ToList();
                        #endregion

                        #region ֱ�ͺ��
                        //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_direct_afterNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_direct_afterNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_direct_afterNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }

                        var round_direct_afterNum = all_direct_afterNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ǰ��ֱ��ǰ��
                        //�洢��һ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_direct_before_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_direct_before_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_direct_before_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�ֱ��ǰ�����
                        var lastRound_direct_before_Number = all_last_direct_before_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ǰ��ֱ�ͺ��
                        //�洢��һ��Ŀÿ���ͷ�12����ֱ�ͺ�������
                        List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                        //�����ͷ�����

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_direct_after_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_direct_after_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_direct_after_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�ֱ�ͺ�����
                        var lastRound_direct_after_Number = all_last_direct_after_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region AI��y
                        EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //�洢ÿ���ͷ�12���µ�AI��������
                        List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;

                                if (psi_AINumber.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ�code�ͷ���code���
                                    var validCombination = psi_AINumber.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_sapcode") &&
                        entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // ƥ�� sfa_zcusmodel_code
                        entity.Contains("sfa_c_sapcode") &&
                        entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // ƥ�� sfa_kunnr_code
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_monthAINum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_monthAINum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_monthAINum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�AI�������
                        var round_AINumber = all_monthAINum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region �g��,�g��,������
                        //�����Ŵ���������б�
                        List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                        // ����һ����Ōg�ӽ�����б�
                        List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                        // ����һ����Ōg��������б�
                        List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                        //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ����όg���Ʃ`�֥���ж�Ӧ�ļ�¼
                        EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ�������ж�Ӧ�ļ�¼
                        EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                        // ���·ݺ�sap_p_code���з���
                        var achi_monthlySums = psi_achi_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                    GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // ����sfa_grossprofit�ܺ�
                                }
                            );
                        // ���·ݺ�sap_p_code���з���
                        var des_monthlySums = psi_Des_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                    GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // ����sfa_grossprofit�ܺ�
                                }
                            );



                        // ���ͷ�codeListת��ΪHashSetList
                        HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                        foreach (var scode in sfa_p_code_hashSet)
                        {
                            // ����g����ë�����g�ӵ�ֵ
                            for (int m = 1; m <= 12; m++)
                            {
                                // ��ȡachi��des�ж�Ӧ�·ݵ�����
                                string p_code = "";
                                decimal achiQuantity = 0;
                                decimal desQuantity = 0;
                                decimal achiGrossProfit = 0, desGrossProfit = 0;
                                p_code = scode;
                                // ���achi_monthlySums�д��ڸ��·ݵ����ݣ����ȡ������ë��
                                if (achi_monthlySums.ContainsKey((m, scode)))
                                {
                                    achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                    achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                                }

                                // ���des_monthlySums�д��ڸ��·ݵ����ݣ����ȡ����
                                if (des_monthlySums.ContainsKey((m, scode)))
                                {
                                    desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                    desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                                }
                                // ���������
                                decimal totalQuantity = achiQuantity + desQuantity;
                                //��ë�����
                                decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                                //�g��ֵ
                                decimal totalAchiQuantity = achiQuantity;
                                // ����һ��ModelMonthNum���󲢽���������б�
                                achi_des_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalQuantity
                                });
                                achi_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalAchiQuantity
                                });
                                grossProfit_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalGrossProfit
                                });
                            }

                        }
                        #endregion

                        #region ͬ��

                        //������ȡ���ͷ���KeyCode����
                        HashSet<string> models_kecode_list = new HashSet<string>();
                        List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                        foreach (var kecode_record in psi_allModels_keycode.Entities)
                        {
                            if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                            {
                                models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                            }
                        }

                        //���ݻ�ȡ����KeyCodeƴ��ΪXML������KeyCodeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                        string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                        //����KeycdoeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                        EntityCollection psi_keycode_allLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModels", model_kecode_fetchxml), serviceClient);

                        for (int i = 1; i <= 12; i++)
                        {
                            foreach (var record in psi_keycode_allLegal.Entities)
                            {
                                if (record.Contains("sfa_sapcode") && record.Contains("sfa_keycode"))
                                {
                                    models_kecode.Add(new ModelMonthNum
                                    {
                                        Model = record["sfa_sapcode"].ToString(),
                                        Month = i,
                                        Keycode = record["sfa_keycode"].ToString(),
                                        ModelSum = 0
                                    });
                                }
                            }
                        }

                        //�� psi_keycode_allModels����ȡ�ͷ���sfa_sapcode��ƴ�ӳɲ�ѯʵ����������е�XML ����
                        string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("",
    psi_keycode_allLegal.Entities
        .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // ����Ƿ���� sfa_sapcode ����
        .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>")
)}</condition>";

                        //����allModel_kecode_fetchxml��sfa_fetch_psi_legalcode_xml��ʵ�����������в�ѯȥ���Ӧ������
                        EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        // ����SapCode���·ݷ��鲢����ʵ�������������ܺ�
                        var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // �� Keycode ���·ݷ���
                            .ToDictionary(
                                group => group.Key, // ����ļ�Ϊ Keycode �� Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                            );
                        var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // �� Keycode ���·ݷ���
                            .ToDictionary(
                                group => group.Key, // ����ļ�Ϊ Keycode �� Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                            );

                        //��achiGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model ��Ӧ sapcode
                                Month = model.Month
                            };

                            if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum = value; // ��ֵ��Ӧ���ܺ�
                            }
                        }

                        //��desGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model ��Ӧ sapcode
                                Month = model.Month
                            };

                            if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum += value; // ��ֵ���
                            }
                        }
                        var mergedModelsKecode = models_kecode
            .GroupBy(item => new { item.Keycode, item.Month }) // �� Keycode �� Month ����
            .Select(group => new ModelMonthNum
            {
                Keycode = group.Key.Keycode,
                Month = group.Key.Month,
                Model = string.Join(";", group.Select(x => x.Model)), // �ϲ� Model ֵ������Ҫ��
                ModelSum = group.Sum(x => x.ModelSum) // ���� ModelSum
            })
            .ToList();

                        #endregion

                        //���������
                        foreach (var item in round_Number)
                        {
                            // ����һ������
                            DataRow newRow = dt.NewRow();
                            #region ��غϼ�
                            // Ϊ sfa_month �и�ֵ���·�
                            newRow["sfa_month"] = item.Month; // ��ֵ�·�

                            // Ϊ sfa_p_names �и�ֵ��Model
                            newRow["sfa_p_names"] = item.Model; // ��ֵ Model

                            // Ϊ sfa_all_sum_number �и�ֵ��ModelSum������ round_Number��
                            newRow["sfa_all_sum_number"] = item.ModelSum; // ��ֵ round_Number �� ModelSum
                            #endregion
                            //�����ͷ���item.Model��ֵ��psi_ModelsAndLegal��sfa_p_code��ֵ����ƥ��
                            foreach (var pname in psi_ModelsAndLegal.Entities)
                            {
                                if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                                {
                                    newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // ��ֵ direct_beforeNumber �� ModelSum
                                    newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // ��ֵ direct_beforeNumber �� ModelSum
                                    break;
                                }

                            }
                            #region ǰ�غϼ�
                            // ���Ҷ�Ӧ�� lastRound_Number ����
                            var lastRound = lastRound_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (lastRound != null)
                            {
                                newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_allsumnumber_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ�ز�
                            // ����ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                            // Ϊ sfa_num_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_num_balance"] = previousDiff;
                            #endregion
                            #region ����
                            // ���Ҷ�Ӧ��Ԥ������
                            var budgetDetail = eachModel_BugetNumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                            if (budgetDetail != null)
                            {
                                newRow["sfa_budget_num"] = budgetDetail.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_budget_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ͬ��
                            // ���Ҷ�Ӧ��ͬ������
                            var samePeriod_num = mergedModelsKecode
                                                .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                            // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                            if (samePeriod_num != null)
                            {
                                newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_sameperiod_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ��ǰ��
                            // ���Ҷ�Ӧ��ֱ��ǰ������
                            var direct_beforeNumber = round_direct_beforeNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_before"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ�ͺ��
                            // ���Ҷ�Ӧ��ֱ�ͺ������
                            var direct_afterNumber = round_direct_afterNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                            if (direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_after"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ��ֱ��ǰ��
                            // ���Ҷ�Ӧ��ֱ��ǰ������
                            var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (lastRound_direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_before_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ��ֱ�����
                            // ���Ҷ�Ӧ��ֱ�ͺ������
                            var last_direct_afterNumber = lastRound_direct_after_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                            if (last_direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_after_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ��ǰ���
                            //����ֱ��ǰ���(ֱ��ǰ��-ǰ��ֱ��ǰ��)
                            var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                            // Ϊ sfa_direct_before_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_direct_before_balance"] = before_half_dif;
                            #endregion
                            #region ֱ������
                            //����ֱ������(ֱ�����-ǰ��ֱ�����)
                            var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                            // Ϊ sfa_direct_after_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_direct_after_balance"] = after_half_dif;
                            #endregion
                            #region AI��y
                            // ���Ҷ�Ӧ��AI�������
                            var AI_number = round_AINumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (AI_number != null)
                            {
                                newRow["sfa_ai_num"] = AI_number.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_ai_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �g��
                            var achi_des_record = achi_des_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (achi_des_record != null)
                            {
                                newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_perf_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �g��
                            var achi_record = achi_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (achi_record != null)
                            {
                                newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_retail_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ������
                            var gro_record = grossProfit_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (gro_record != null && achi_des_record.ModelSum != 0)
                            {
                                newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_perf_grossprofit"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �M����
                            //�g��/ǰ�غ�Ӌ
                            if (achi_des_record != null && lastRound.ModelSum != 0)
                            {
                                newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_progressrate"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ������
                            var model_reason = all_ModelReasonList
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (model_reason != null)
                            {
                                newRow["sfa_num_reason"] = model_reason.Reason; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ���������ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_num_reason"] = ""; // ���û���ҵ���Ӧ���ݣ���ֵΪ "" ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ��������
                            var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                            if (Adjust_num != null)
                            {
                                newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_adjusted_quantities"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ���ɱ���

                            #endregion
                            // ��������ӵ� DataTable
                            dt.Rows.Add(newRow);
                        }
                    }
                    #endregion
                    #region �õ���ǰ�˴����ͷ���ƥ�������
                    else //�������ͬ��
                    {
                        //��ͬ���ֵ��PSI��ϸ������ȡ������ͬ����Ӹ������л�ȡ
                        //����ͬ����ͷ�����ת�ɲ�ѯPSI��ϸ���xml
                        string PSI_Details_Models_FetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("", commonItems.Select(item => $"<value>{item}</value>"))}</condition>";

                        //��ѯ��ͬ�ͷ���PSI��ϸ���еļ�¼
                        EntityCollection entityCollection_PSI_DetailsCommonRecord = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_PSI_Deatils", main_PSI_IdXml, PSI_Details_Models_FetchXml)));
                        #region ����ƥ����ͬ���ͷ�
                        for (int m = 1; m <= 12; m++)
                        {

                            bool flag = false;
                            foreach (var commonModel in commonItems)
                            {
                                foreach (var common_record in entityCollection_PSI_DetailsCommonRecord.Entities)
                                {
                                    if (common_record.Contains("sfa_month") && common_record.Contains("sfa_p_names") && Convert.ToInt32(common_record["sfa_month"].ToString()) == m && common_record["sfa_p_names"].ToString() == commonModel)
                                    {
                                        DataRow newRow = dt.NewRow();
                                        // Ϊ sfa_month �и�ֵ���·�
                                        newRow["sfa_month"] = m; // ��ֵ�·�
                                        // Ϊ sfa_p_names �и�ֵ��Model
                                        newRow["sfa_p_names"] = common_record["sfa_p_names"].ToString();// ��ֵ Model
                                        //�ͷ�code
                                        newRow["sfa_p_sapcode"] = common_record.Contains("sfa_p_sapcode") ? common_record["sfa_p_sapcode"].ToString() : "";
                                        //����name
                                        newRow["sfa_c_names"] = common_record["sfa_c_names"].ToString();
                                        // ��غϼ�
                                        newRow["sfa_all_sum_number"] = Convert.ToDecimal(common_record["sfa_all_sum_number"]);
                                        //ǰ�غϼ�                                                                   
                                        newRow["sfa_allsumnumber_pre"] = Convert.ToDecimal(common_record["sfa_allsumnumber_pre"]);
                                        //ǰ�ز�
                                        newRow["sfa_num_balance"] = Convert.ToDecimal(common_record["sfa_num_balance"]);
                                        //����
                                        newRow["sfa_budget_num"] = Convert.ToDecimal(common_record["sfa_budget_num"]);
                                        //ͬ��
                                        newRow["sfa_sameperiod_num"] = Convert.ToDecimal(common_record["sfa_sameperiod_num"]);
                                        //ֱ��ǰ��
                                        newRow["sfa_direct_before"] = Convert.ToDecimal(common_record["sfa_direct_before"]);
                                        //ֱ�����
                                        newRow["sfa_direct_after"] = Convert.ToDecimal(common_record["sfa_direct_after"]);
                                        //ǰ��ֱ��ǰ��
                                        newRow["sfa_direct_before_pre"] = Convert.ToDecimal(common_record["sfa_direct_before_pre"]);
                                        //ǰ��ֱ�����
                                        newRow["sfa_direct_after_pre"] = Convert.ToDecimal(common_record["sfa_direct_after_pre"]);
                                        //ֱ��ǰ���
                                        newRow["sfa_direct_before_balance"] = Convert.ToDecimal(common_record["sfa_direct_before_balance"]);
                                        //ֱ������
                                        newRow["sfa_direct_after_balance"] = Convert.ToDecimal(common_record["sfa_direct_after_balance"]);
                                        //AI��y
                                        newRow["sfa_ai_num"] = Convert.ToDecimal(common_record["sfa_ai_num"]);
                                        //�g��
                                        newRow["sfa_sales_perf_num"] = Convert.ToDecimal(common_record["sfa_sales_perf_num"]);
                                        //�g��
                                        newRow["sfa_sales_retail_num"] = Convert.ToDecimal(common_record["sfa_sales_retail_num"]);
                                        //������
                                        newRow["sfa_perf_grossprofit"] = Convert.ToDecimal(common_record["sfa_perf_grossprofit"]);
                                        //�M����
                                        newRow["sfa_progressrate"] = Convert.ToDecimal(common_record["sfa_progressrate"]);
                                        //������
                                        newRow["sfa_num_reason"] = common_record.Contains("sfa_num_reason") ? common_record["sfa_num_reason"].ToString() : "";
                                        //��������
                                        newRow["sfa_adjusted_quantities"] = Convert.ToDecimal(common_record["sfa_adjusted_quantities"]);
                                        dt.Rows.Add(newRow);
                                        flag = true;
                                    }
                                    //Ϊȱʧ�·ݸ�ֵ
                                    if (!flag)
                                    {
                                        DataRow newRow = dt.NewRow();
                                        // Ϊ sfa_month �и�ֵ���·�
                                        newRow["sfa_month"] = m; // ��ֵ�·�
                                        // Ϊ sfa_p_names �и�ֵ��Model
                                        newRow["sfa_p_names"] = commonModel;// ��ֵ Model
                                        //�ͷ�code
                                        newRow["sfa_p_sapcode"] = entityCollection_PSI_DetailsCommonRecord.Entities
    .Where(e => e.Contains("sfa_p_names") && e["sfa_p_names"] != null && e["sfa_p_names"].ToString() == commonModel)
    .Select(e => e.Contains("sfa_p_code") && e["sfa_p_code"] != null ? e["sfa_p_code"].ToString() : null)
    .FirstOrDefault();
                                        //����name
                                        newRow["sfa_c_names"] = "PSI";
                                        // ��غϼ�0
                                        newRow["sfa_all_sum_number"] = Convert.ToDecimal(0);
                                        //ǰ�غϼ�                                                                   
                                        newRow["sfa_allsumnumber_pre"] = Convert.ToDecimal(0);
                                        //ǰ�ز�
                                        newRow["sfa_num_balance"] = Convert.ToDecimal(0);
                                        //����
                                        newRow["sfa_budget_num"] = Convert.ToDecimal(0);
                                        //ͬ��
                                        newRow["sfa_sameperiod_num"] = Convert.ToDecimal(0);
                                        //ֱ��ǰ��
                                        newRow["sfa_direct_before"] = Convert.ToDecimal(0);
                                        //ֱ�����
                                        newRow["sfa_direct_after"] = Convert.ToDecimal(0);
                                        //ǰ��ֱ��ǰ��
                                        newRow["sfa_direct_before_pre"] = Convert.ToDecimal(0);
                                        //ǰ��ֱ�����
                                        newRow["sfa_direct_after_pre"] = Convert.ToDecimal(0);
                                        //ֱ��ǰ���
                                        newRow["sfa_direct_before_balance"] = Convert.ToDecimal(0);
                                        //ֱ������
                                        newRow["sfa_direct_after_balance"] = Convert.ToDecimal(0);
                                        //AI��y
                                        newRow["sfa_ai_num"] = Convert.ToDecimal(0);
                                        //�g��
                                        newRow["sfa_sales_perf_num"] = Convert.ToDecimal(0);
                                        //�g��
                                        newRow["sfa_sales_retail_num"] = Convert.ToDecimal(0);
                                        //������
                                        newRow["sfa_perf_grossprofit"] = Convert.ToDecimal(0);
                                        //�M����
                                        newRow["sfa_progressrate"] = Convert.ToDecimal(0);
                                        //������
                                        newRow["sfa_num_reason"] = "";
                                        //��������
                                        newRow["sfa_adjusted_quantities"] = Convert.ToDecimal(0);
                                        dt.Rows.Add(newRow);
                                    }
                                }
                            }
                        }
                        #endregion
                        #region ����ƥ�䲻ͬ���ͷ�
                        //����ͬ����ͷ�����ת�ɲ�ѯ���������xml
                        string PSI_Diff_Models_FetchXml = $"<condition attribute='sfa_name' operator='in'>{string.Join("", differentItems.Select(item => $"<value>{item}</value>"))}</condition>";
                        //����ͬ����ͷ�����ת�ɲ�ѯPSI��ϸ���xml
                        string PSI_Details_DiffModels_FetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("", differentItems.Select(item => $"<value>{item}</value>"))}</condition>";
                        //�ӵ�������л�ȡ��ͬ�ͷ���Ӧ�ķ������
                        EntityCollection PSI_DiffModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI("B001", yearXml, sfa_fetch_legalName, PSI_Diff_Models_FetchXml), serviceClient);
                        //�洢PSI�ͷ����ͷ���Ӧ�ķ���
                        var psi_Diff_ModelsAndlegal = PSI_DiffModelsAndLegal.Entities
            .Where(entity => entity.Contains("sfa_kunnr") && entity.Contains("sfa_zcusmodel"))
            .Select(entity => new
            {
                sfa_kunnr = ((EntityReference)entity["sfa_kunnr"]).Name,  // ��ȡ sfa_kunnr �� Name
                sfa_kunnr_code = entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode").Value,
                sfa_zcusmodel = ((EntityReference)entity["sfa_zcusmodel"]).Name,  // ��ȡ sfa_zcusmodel �� Name
                sfa_zcusmodel_code = entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode").Value  // ��ȡ sfa_zcusmodel �� Name

            })
            .ToList();
                        #region ��������

                        //���ݵ�¼���ţ��ͷ����·ݴ�PSIԔ�����в�ѯ
                        EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", PSI_Details_DiffModels_FetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                        //�洢12���¶�Ӧ�ͷ��ĵ���������List
                        List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                        foreach (var model in psi_allModelsList)
                        {
                            for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                            {
                                bool flag = true;
                                foreach (var adjust_record in psi_adjust_num.Entities)
                                {
                                    //�Ƚϼ�¼�е��ͷ����·ݣ��Լ��Ƿ����sfa_adjusted_quantities����
                                    if (adjust_record.Contains("sfa_p_names") && model == adjust_record.Contains("sfa_p_names").ToString() && adjust_record.Contains("sfa_adjusted_quantities") && adjust_record.Contains("sfa_month") && Convert.ToInt32(adjust_record.Contains("sfa_month").ToString()) == adjust_month)
                                    {
                                        all_month_AdjustNum.Add(new ModelMonthNum
                                        {
                                            Model = adjust_record["sfa_p_names"].ToString(),
                                            Month = adjust_month,
                                            ModelSum = Convert.ToDecimal(adjust_record["sfa_adjusted_quantities"])
                                        });
                                        flag = false;
                                        break;
                                    }
                                }
                                if (flag)
                                    all_month_AdjustNum.Add(new ModelMonthNum
                                    {
                                        Model = model,
                                        Month = adjust_month,
                                        ModelSum = 0
                                    });
                            }
                        }
                        #endregion

                        #region ��غ�Ӌ,������

                        //�洢ÿ���ͷ�12���µ�����
                        List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                        //�洢ÿ���ͷ�12���µĥ�����
                        List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                        //������ͬ�ͷ����ͷ��ͷ��˵�ȫ�����
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                                });
                                                all_ModelReasonList.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    Reason = validEntity.Contains("sfa_num_reason") ? validEntity["sfa_num"].ToString() : ""
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                        all_ModelReasonList.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            Reason = ""
                                        });
                                    }
                                }
                                else
                                {
                                    all_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                    all_ModelReasonList.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        Reason = ""
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�����������
                        var round_Number = all_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        var all_reason = all_ModelReasonList
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                    })
                    .ToList();
                        #endregion

                        #region ǰ�غ�Ӌ
                        //����versionNumber��PSI���˻�ȡ��һ��Ŀʮ�����·ݴ����ͷ��ļ�¼
                        EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                        //�洢��һ��Ŀÿ���ͷ�12���µ�����
                        List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                        //����ǰ�˴����ͷ��ͷ��˵�ȫ�����
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_num") ? Convert.ToDecimal(validEntity["sfa_num"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        var lastRound_Number = all_last_monthNum
                   .GroupBy(x => new { x.Model, x.Month })
                   .Select(g => new ModelMonthNum
                   {
                       Model = g.Key.Model,
                       Month = g.Key.Month,
                       ModelSum = g.Sum(x => x.ModelSum)
                   })
                   .ToList();
                        #endregion

                        #region ����
                        //��ȡ�������°汾
                        EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                        //�洢ÿ���ͷ�12���µ���������
                        List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                        //����-�汾��
                        string versionnumber = "";
                        if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                        {
                            var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                            //����-�汾��
                            versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                        }
                        else
                        {
                            return new BadRequestObjectResult(new VersionControlResponse
                            {
                                Status = StatusCodes.Status500InternalServerError.ToString(),
                                Message = "�x�k�������ˤˤ��Є��ͷ������ڤ��Ƥ��ޤ��󡢌�������_�J���Ƥ�������"
                            });
                        }

                        //��PSIΪtrue�ķ�����Ϊ��ѯ������ϸ�Ĵ洢����ת��ΪFetchXML
                        string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.Select(item => $"<value>{item}</value>"))}</condition>";
                        //��Ѱ��ǰ�汾num��xml
                        string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                        //��ȡ������ϸ
                        EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (result_Budget_BVersion.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = result_Budget_BVersion.Entities
                    .Where(entity =>
                        entity.Contains("sfa_modelname") &&
                        entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_legalname") &&
                        entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_budget_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_modelname"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_budget_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_budget_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ������������
                        var eachModel_BugetNumber = all_budget_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ֱ��ǰ��
                        //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_direct_beforeNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_direct_beforeNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_direct_beforeNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        var round_direct_beforeNum = all_direct_beforeNum
                   .GroupBy(x => new { x.Model, x.Month })
                   .Select(g => new ModelMonthNum
                   {
                       Model = g.Key.Model,
                       Month = g.Key.Month,
                       ModelSum = g.Sum(x => x.ModelSum)
                   })
                   .ToList();
                        #endregion

                        #region ֱ�ͺ��
                        //�洢��ǰ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_direct_afterNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_direct_afterNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_direct_afterNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }

                        var round_direct_afterNum = all_direct_afterNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ǰ��ֱ��ǰ��
                        //�洢��һ��Ŀÿ���ͷ�12����ֱ��ǰ�������
                        List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_direct_before_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_before") ? Convert.ToDecimal(validEntity["sfa_direct_before"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_direct_before_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_direct_before_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�ֱ��ǰ�����
                        var lastRound_direct_before_Number = all_last_direct_before_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region ǰ��ֱ�ͺ��
                        //�洢��һ��Ŀÿ���ͷ�12����ֱ�ͺ�������
                        List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                        //�����ͷ�����

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //�ȽϹ���������д��ڵļ�¼
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ��ͷ������
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // ƥ�� sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // ƥ�� sfa_kunnr
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_last_direct_after_monthNum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_direct_after") ? Convert.ToDecimal(validEntity["sfa_direct_after"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_last_direct_after_monthNum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_last_direct_after_monthNum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�ֱ�ͺ�����
                        var lastRound_direct_after_Number = all_last_direct_after_monthNum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region AI��y
                        EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //�洢ÿ���ͷ�12���µ�AI��������
                        List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m����12���·�
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;

                                if (psi_AINumber.Entities.Count != 0)
                                {
                                    //��ȡ��Ч���ͷ�code�ͷ���code���
                                    var validCombination = psi_AINumber.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_sapcode") &&
                        entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // ƥ�� sfa_zcusmodel_code
                        entity.Contains("sfa_c_sapcode") &&
                        entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // ƥ�� sfa_kunnr_code
                    .ToList();
                                    //����з��Ͻ��ж�Ӧ��¼�����
                                    if (validCombination.Any())
                                    {
                                        // ������Ч����ϣ����к�������
                                        foreach (var validEntity in validCombination)
                                        {
                                            if (validEntity.Contains("sfa_month") && Convert.ToInt32(validEntity["sfa_month"]) == m)
                                            {
                                                all_monthAINum.Add(new ModelMonthNum
                                                {
                                                    Model = validEntity["sfa_p_name"].ToString(),
                                                    Month = m,
                                                    ModelSum = validEntity.Contains("sfa_quantity") ? Convert.ToDecimal(validEntity["sfa_quantity"]) : 0
                                                });
                                                flag = true;
                                            }
                                        }
                                        //���flagΪfalse˵��û�ж�Ӧ���·�ֵ����û�ж�Ӧ���ͷ��ͷ������
                                    }
                                    if (!flag)
                                    {
                                        all_monthAINum.Add(new ModelMonthNum
                                        {
                                            Model = model_legal.sfa_zcusmodel,
                                            Month = m,
                                            ModelSum = 0
                                        });
                                    }
                                }
                                else
                                {
                                    all_monthAINum.Add(new ModelMonthNum
                                    {
                                        Model = model_legal.sfa_zcusmodel,
                                        Month = m,
                                        ModelSum = 0
                                    });
                                }
                            }
                        }
                        //�����ͷ����·ݽ�AI�������
                        var round_AINumber = all_monthAINum
                    .GroupBy(x => new { x.Model, x.Month })
                    .Select(g => new ModelMonthNum
                    {
                        Model = g.Key.Model,
                        Month = g.Key.Month,
                        ModelSum = g.Sum(x => x.ModelSum)
                    })
                    .ToList();
                        #endregion

                        #region �g��,�g��,������
                        //�����Ŵ���������б�
                        List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                        // ����һ����Ōg�ӽ�����б�
                        List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                        // ����һ����Ōg��������б�
                        List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                        //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ����όg���Ʃ`�֥���ж�Ӧ�ļ�¼
                        EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //������ȣ��·ݣ��ͷ���SAPcode�ͷ��˵�SAPcode�õ�������ж�Ӧ�ļ�¼
                        EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                        // ���·ݺ�sap_p_code���з���
                        var achi_monthlySums = psi_achi_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                    GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // ����sfa_grossprofit�ܺ�
                                }
                            );
                        // ���·ݺ�sap_p_code���з���
                        var des_monthlySums = psi_Des_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // ���·ݺ�sap_p_code����
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // ʹ��Ԫ����Ϊ�ֵ�ļ�
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // ����sfa_quantity�ܺ�
                                    GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // ����sfa_grossprofit�ܺ�
                                }
                            );



                        // ���ͷ�codeListת��ΪHashSetList
                        HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                        foreach (var scode in sfa_p_code_hashSet)
                        {
                            // ����g����ë�����g�ӵ�ֵ
                            for (int m = 1; m <= 12; m++)
                            {
                                // ��ȡachi��des�ж�Ӧ�·ݵ�����
                                string p_code = "";
                                decimal achiQuantity = 0;
                                decimal desQuantity = 0;
                                decimal achiGrossProfit = 0, desGrossProfit = 0;
                                p_code = scode;
                                // ���achi_monthlySums�д��ڸ��·ݵ����ݣ����ȡ������ë��
                                if (achi_monthlySums.ContainsKey((m, scode)))
                                {
                                    achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                    achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                                }

                                // ���des_monthlySums�д��ڸ��·ݵ����ݣ����ȡ����
                                if (des_monthlySums.ContainsKey((m, scode)))
                                {
                                    desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                    desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                                }
                                // ���������
                                decimal totalQuantity = achiQuantity + desQuantity;
                                //��ë�����
                                decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                                //�g��ֵ
                                decimal totalAchiQuantity = achiQuantity;
                                // ����һ��ModelMonthNum���󲢽���������б�
                                achi_des_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalQuantity
                                });
                                achi_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalAchiQuantity
                                });
                                grossProfit_num.Add(new ModelMonthNum
                                {
                                    Model = p_code,
                                    Month = m,
                                    ModelSum = totalGrossProfit
                                });
                            }

                        }
                        #endregion

                        #region ͬ��

                        //������ȡ���ͷ���KeyCode����
                        HashSet<string> models_kecode_list = new HashSet<string>();
                        List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                        foreach (var kecode_record in psi_allModels_keycode.Entities)
                        {
                            if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                            {
                                models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                            }
                        }

                        //���ݻ�ȡ����KeyCodeƴ��ΪXML������KeyCodeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                        string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                        //����KeycdoeXML��ȡ��Ʒ���ж�Ӧ���ͷ���Sapcode
                        EntityCollection psi_keycode_allLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModels", model_kecode_fetchxml), serviceClient);

                        for (int i = 1; i <= 12; i++)
                        {
                            foreach (var record in psi_keycode_allLegal.Entities)
                            {
                                if (record.Contains("sfa_sapcode") && record.Contains("sfa_keycode"))
                                {
                                    models_kecode.Add(new ModelMonthNum
                                    {
                                        Model = record["sfa_sapcode"].ToString(),
                                        Month = i,
                                        Keycode = record["sfa_keycode"].ToString(),
                                        ModelSum = 0
                                    });
                                }
                            }
                        }

                        //�� psi_keycode_allModels����ȡ�ͷ���sfa_sapcode��ƴ�ӳɲ�ѯʵ����������е�XML ����
                        string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("", psi_keycode_allLegal.Entities
        .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // ����Ƿ���� sfa_sapcode ����
        .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>"))}</condition>";

                        //����allModel_kecode_fetchxml��sfa_fetch_psi_legalcode_xml��ʵ�����������в�ѯȥ���Ӧ������
                        EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        // ����SapCode���·ݷ��鲢����ʵ�������������ܺ�
                        var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // �� Keycode ���·ݷ���
                            .ToDictionary(
                                group => group.Key, // ����ļ�Ϊ Keycode �� Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                            );
                        var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // ȷ���ֶδ���
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // �� Keycode ���·ݷ���
                            .ToDictionary(
                                group => group.Key, // ����ļ�Ϊ Keycode �� Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // ����ÿ��� sfa_quantity �ܺ�
                            );

                        //��achiGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model ��Ӧ sapcode
                                Month = model.Month
                            };

                            if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum = value; // ��ֵ��Ӧ���ܺ�
                            }
                        }

                        //��desGroupedByKeycodeAndMonth��models_kecode�и���sapcode��month��Ӧ��ModelSum��ֵ���и�ֵ
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model ��Ӧ sapcode
                                Month = model.Month
                            };

                            if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum += value; // ��ֵ���
                            }
                        }
                        var mergedModelsKecode = models_kecode
            .GroupBy(item => new { item.Keycode, item.Month }) // �� Keycode �� Month ����
            .Select(group => new ModelMonthNum
            {
                Keycode = group.Key.Keycode,
                Month = group.Key.Month,
                Model = string.Join(";", group.Select(x => x.Model)), // �ϲ� Model ֵ������Ҫ��
                ModelSum = group.Sum(x => x.ModelSum) // ���� ModelSum
            })
            .ToList();

                        #endregion

                        //���������
                        foreach (var item in round_Number)
                        {
                            // ����һ������
                            DataRow newRow = dt.NewRow();
                            #region ��غϼ�
                            // Ϊ sfa_month �и�ֵ���·�
                            newRow["sfa_month"] = item.Month; // ��ֵ�·�

                            // Ϊ sfa_p_names �и�ֵ��Model
                            newRow["sfa_p_names"] = item.Model; // ��ֵ Model

                            // Ϊ sfa_all_sum_number �и�ֵ��ModelSum������ round_Number��
                            newRow["sfa_all_sum_number"] = item.ModelSum; // ��ֵ round_Number �� ModelSum
                            #endregion
                            //�����ͷ���item.Model��ֵ��psi_ModelsAndLegal��sfa_p_code��ֵ����ƥ��
                            foreach (var pname in psi_ModelsAndLegal.Entities)
                            {
                                if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                                {
                                    newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // ��ֵ direct_beforeNumber �� ModelSum
                                    newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // ��ֵ direct_beforeNumber �� ModelSum
                                    break;
                                }

                            }
                            #region ǰ�غϼ�
                            // ���Ҷ�Ӧ�� lastRound_Number ����
                            var lastRound = lastRound_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (lastRound != null)
                            {
                                newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_allsumnumber_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ�ز�
                            // ����ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                            // Ϊ sfa_num_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_num_balance"] = previousDiff;
                            #endregion
                            #region ����
                            // ���Ҷ�Ӧ��Ԥ������
                            var budgetDetail = eachModel_BugetNumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                            if (budgetDetail != null)
                            {
                                newRow["sfa_budget_num"] = budgetDetail.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_budget_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ͬ��
                            // ���Ҷ�Ӧ��ͬ������
                            var samePeriod_num = mergedModelsKecode
                                                .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                            // ����ҵ���Ӧ��Ԥ�����ݣ���ΪԤ���и�ֵ
                            if (samePeriod_num != null)
                            {
                                newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��Ԥ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_sameperiod_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ��ǰ��
                            // ���Ҷ�Ӧ��ֱ��ǰ������
                            var direct_beforeNumber = round_direct_beforeNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_before"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ�ͺ��
                            // ���Ҷ�Ӧ��ֱ�ͺ������
                            var direct_afterNumber = round_direct_afterNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                            if (direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_after"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ��ֱ��ǰ��
                            // ���Ҷ�Ӧ��ֱ��ǰ������
                            var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (lastRound_direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_before_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ǰ��ֱ�����
                            // ���Ҷ�Ӧ��ֱ�ͺ������
                            var last_direct_afterNumber = lastRound_direct_after_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ�ͺ�����ݣ���Ϊֱ�ͺ���и�ֵ
                            if (last_direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // ��ֵ direct_afterNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ�ͺ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_direct_after_pre"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ֱ��ǰ���
                            //����ֱ��ǰ���(ֱ��ǰ��-ǰ��ֱ��ǰ��)
                            var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                            // Ϊ sfa_direct_before_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_direct_before_balance"] = before_half_dif;
                            #endregion
                            #region ֱ������
                            //����ֱ������(ֱ�����-ǰ��ֱ�����)
                            var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                            // Ϊ sfa_direct_after_balance �и�ֵ��ǰ�ز��غ�Ӌ - ǰ�غ�Ӌ��
                            newRow["sfa_direct_after_balance"] = after_half_dif;
                            #endregion
                            #region AI��y
                            // ���Ҷ�Ӧ��AI�������
                            var AI_number = round_AINumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (AI_number != null)
                            {
                                newRow["sfa_ai_num"] = AI_number.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_ai_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �g��
                            var achi_des_record = achi_des_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (achi_des_record != null)
                            {
                                newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_perf_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �g��
                            var achi_record = achi_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (achi_record != null)
                            {
                                newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_retail_num"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ������
                            var gro_record = grossProfit_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // ����ҵ��˶�Ӧ�� lastRound ���ݣ���Ϊ sfa_allsumnumber_pre �и�ֵ
                            if (gro_record != null && achi_des_record.ModelSum != 0)
                            {
                                newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_perf_grossprofit"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region �M����
                            //�g��/ǰ�غ�Ӌ
                            if (achi_des_record != null && lastRound.ModelSum != 0)
                            {
                                newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // ��ֵ lastRound_Number �� ModelSum
                            }
                            else
                            {
                                newRow["sfa_progressrate"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ������
                            var model_reason = all_ModelReasonList
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // ����ҵ���Ӧ��ֱ��ǰ�����ݣ���Ϊֱ��ǰ���и�ֵ
                            if (model_reason != null)
                            {
                                newRow["sfa_num_reason"] = model_reason.Reason; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ���������ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_num_reason"] = ""; // ���û���ҵ���Ӧ���ݣ���ֵΪ "" ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ��������
                            var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                            if (Adjust_num != null)
                            {
                                newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // ��ֵ direct_beforeNumber �� ModelSum
                            }
                            else
                            {
                                // ���û���ҵ���Ӧ��ֱ��ǰ�����ݣ�����Ϊ0��������Ĭ��ֵ
                                newRow["sfa_adjusted_quantities"] = 0; // ���û���ҵ���Ӧ���ݣ���ֵΪ 0 ��������Ĭ��ֵ
                            }
                            #endregion
                            #region ���ɱ���

                            #endregion
                            // ��������ӵ� DataTable
                            dt.Rows.Add(newRow);
                        }
                    }
                    #endregion

                    #endregion

                }
                OutputDetails outputDetails = new OutputDetails();
                var response = outputDetails.FormatResult(dt, mainRollingBudgetId.ToString());
                if (mainRollingBudgetId != null)
                {
                    return new OkObjectResult(response);
                }

            }
            catch (Exception ex)
            {
                LogHelper.WriteLog(ex, req);
                log.LogInformation("Error:" + ex.Message);

                //return new NotFoundResult();
                return new NotFoundObjectResult(ex);
            }

            return new OkObjectResult("");
        }
        public class OutputDetails
        {
            //����ǰ���ض���ʽ
            public List<object> FormatResult(DataTable dt, string applicationNumber)
            {
                var results = new List<object>();
                var models = dt.AsEnumerable().Select(row => row.Field<string>("sfa_p_names")).Distinct();
                var legals = dt.AsEnumerable().Select(row => row.Field<string>("sfa_c_names")).Distinct();

                foreach (var legal in legals)
                {
                    foreach (var model in models)
                    {
                        // ���÷������ͷ��Ƿ���Ч���
                        if (dt.AsEnumerable().Any(row => row.Field<string>("sfa_c_names") == legal && row.Field<string>("sfa_p_names") == model))
                        {
                            results.Add(Create_AdjustNum_Entry(dt, legal, model, applicationNumber, "�{������", "sfa_adjusted_quantities"));
                            results.Add(Create_Num_Entry(dt, legal, model, "��غ�Ӌ", "sfa_all_sum_number"));
                            results.Add(Create_Num_PreEntry(dt, legal, model, "ǰ�غ�Ӌ", "sfa_allsumnumber_pre"));
                            results.Add(Create_Num_BalanceEntry(dt, legal, model, applicationNumber, "ǰ�ز�", "sfa_num_balance"));
                            results.Add(Create_Num_ReasonEntry(dt, legal, model, applicationNumber, "������", "sfa_num_reason"));
                            results.Add(Create_Budget_NumEntry(dt, legal, model, applicationNumber, "����", "sfa_budget_num"));
                            results.Add(Create_SamePeriod_NumEntry(dt, legal, model, applicationNumber, "ͬ��", "sfa_sameperiod_num"));
                            results.Add(Create_PerfNumEntry(dt, legal, model, applicationNumber, "�g������", "sfa_sales_perf_num"));
                            results.Add(Create_Retail_NumEntry(dt, legal, model, applicationNumber, "�g������", "sfa_sales_retail_num"));
                            results.Add(Create_ProgressRate(dt, legal, model, applicationNumber, "�M����", "sfa_progressrate"));
                            results.Add(Create_Perf_GrossprofitEntry(dt, legal, model, applicationNumber, "������", "sfa_perf_grossprofit"));
                            results.Add(Create_BeforeEntry(dt, legal, model, applicationNumber, "ֱ��ǰ��", "sfa_direct_before"));
                            results.Add(Create_AfterEntry(dt, legal, model, applicationNumber, "ֱ�����", "sfa_direct_after"));
                            results.Add(Create_Before_PreEntry(dt, legal, model, "ǰ��ֱ��ǰ��", "sfa_direct_before_pre"));
                            results.Add(Create_Before_BalEntry(dt, legal, model, "ֱ��ǰ���", "sfa_direct_before_balance"));
                            results.Add(Create_After_PreEntry(dt, legal, model, "ǰ��ֱ�����", "sfa_direct_after_pre"));
                            results.Add(Create_After_BalEntry(dt, legal, model, "ֱ������", "sfa_direct_after_balance"));
                            results.Add(Create_AINum_Entry(dt, legal, model, "AI��y", "sfa_ai_num"));
                            results.Add(Create_SRNum_Entry(dt, legal, model, "���ɱ���", ""));

                        }
                    }
                }
                return results; // ���ؽ����
            }
            //�{������
            private object Create_AdjustNum_Entry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            sfa_adjusted_quantities = Convert.ToDecimal(detailRow.Field<object>(columnName)),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //��غϼ�
            private object Create_Num_Entry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            sfa_allsumnumber = Convert.ToDecimal(detailRow.Field<object>(columnName)),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //AI
            private object Create_AINum_Entry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            sfa_ai_num = Convert.ToDecimal(detailRow.Field<object>(columnName)),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //�M����
            private object Create_ProgressRate(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            sfa_progressrate = Convert.ToDecimal(detailRow.Field<object>(columnName)),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //������
            private object Create_Perf_GrossprofitEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            sfa_perf_grossprofit = Convert.ToDecimal(detailRow.Field<object>(columnName)),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //������
            private object Create_Num_ReasonEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            num_Reason = detailRow.Field<object>(columnName)?.ToString() ?? string.Empty,
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ǰ����
            private object Create_Num_PreEntry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);

                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            num_Pre = (int)Math.Truncate(Convert.ToDouble(detailRow.Field<object>(columnName))),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ǰ�ز�
            private object Create_Num_BalanceEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            num_Balance = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }

                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //Ԥ��
            private object Create_Budget_NumEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            budget_Num = (int)Math.Truncate(Convert.ToDouble(detailRow.Field<object>(columnName))),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ͬ��
            private object Create_SamePeriod_NumEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            samePeriod_Num = (int)Math.Truncate(Convert.ToDouble(detailRow.Field<object>(columnName))),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //�g������
            private object Create_PerfNumEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            perfNum = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //�g������
            private object Create_Retail_NumEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            retail_Num = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ֱ��ǰ��
            private object Create_BeforeEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            berfore = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ֱ�����
            private object Create_AfterEntry(DataTable dt, string legal, string model, string applicationNumber, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            after = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ǰ��ֱ��ǰ��
            private object Create_Before_PreEntry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            before_Pre = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ǰ��ֱ�����
            private object Create_After_PreEntry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            after_Pre = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ֱ��ǰ���
            private object Create_Before_BalEntry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            before_Bal = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //ֱ������
            private object Create_After_BalEntry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    var detailRow = dt.AsEnumerable().FirstOrDefault(r => r.Field<string>("sfa_c_names") == legal && r.Field<string>("sfa_p_names") == model && r.Field<int>("sfa_month") == month);
                    if (detailRow != null)
                    {
                        monthDetails.Add(new
                        {
                            after_Bal = detailRow.Field<object>(columnName),
                            month = month.ToString()
                        });
                    }
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
            //���ɱ���
            private object Create_SRNum_Entry(DataTable dt, string legal, string model, string type, string columnName)
            {
                var monthDetails = new List<object>();
                for (int month = 1; month <= 12; month++)
                {
                    monthDetails.Add(new
                    {
                        sfa_ai_num = 0,
                        month = month.ToString()
                    });
                }
                return new
                {
                    legal = legal,
                    model = model,
                    type = type,
                    currentMonth = DateTime.Now.Month,
                    monthDetails = monthDetails
                };
            }
        }
        // ���� DataTable ����
        public static DataTable CreateDataTableFromDataverseSchema(string entityLogicalName, ServiceClient serviceClient)
        {
            DataTable dt = new DataTable(); // ���� DataTable

            try
            {
                var allAttributes = serviceClient.GetAllAttributesForEntity(entityLogicalName);

                foreach (var attribute in allAttributes)
                {
                    if ((attribute.IsPrimaryId.HasValue && attribute.IsPrimaryId.Value) ||
                        (attribute.IsCustomAttribute.HasValue && attribute.IsCustomAttribute.Value))
                    {
                        string columnName = attribute.LogicalName;
                        if (attribute.AttributeTypeName.Value == "LookupType")
                        {
                            dt.Columns.Add(columnName, typeof(Guid)); // �����ֶ� ID
                            dt.Columns.Add(columnName + "_Formatted", typeof(string)); // �����ֶ�����
                        }
                        else if (attribute.AttributeTypeName.Value == "PicklistType")
                        {
                            dt.Columns.Add(columnName, typeof(int)); // ѡ��ֵ
                            dt.Columns.Add(columnName + "_Formatted", typeof(string)); // ѡ���ı�
                        }
                        else
                        {
                            dt.Columns.Add(columnName, ConvertDataverseTypeToSystemType(attribute.AttributeTypeName.Value));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to create DataTable: " + ex.Message);
            }

            return dt;
        }

        private static Type ConvertDataverseTypeToSystemType(string attributeTypeName)
        {
            return attributeTypeName switch
            {
                "StringType" => typeof(string),
                "IntegerType" => typeof(int),
                "MoneyType" => typeof(decimal),
                "DateTimeType" => typeof(DateTime),
                "BooleanType" => typeof(bool),
                "DoubleType" => typeof(double),
                "DecimalType" => typeof(decimal),
                _ => typeof(string) // Ĭ������
            };
        }
    }
}


public class ModelMonthNum
{
    public string Model { get; set; }

    public int Month { get; set; }
    public decimal ModelSum { get; set; }

    public string Keycode { get; set; }

    public string Reason { get; set; }
}
