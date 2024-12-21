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
    /// PSI个别做成
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
                // 创建服务客户端
                using ServiceClient serviceClient = new ServiceClient(CDSHelper.GetDataverseConnectionString());
                if (!serviceClient.IsReady)
                {
                    log.LogError($"【服务器内部错误】服务器连接有误");
                    return new BadRequestObjectResult(new VersionControlResponse
                    {
                        Status = StatusCodes.Status500InternalServerError.ToString(),
                        Message = "サーバー内部エラーが発生しました。"
                    });
                }

                #region 从前端获取信息
                //获取滚动予算的信息
                var Rolling_Detail = data?.detail ?? null;
                //获取登录番号
                var mainRollingBudgetId = Rolling_Detail[0]?.applicationNumber ?? string.Empty;
                //获取型番
                var psi_allModels = data?.Models ?? string.Empty;
                //获取年份
                int Year = Convert.ToInt32(Rolling_Detail[0].year ?? 0);
                //月份
                int[] month = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
                //获取版本
                string round = Rolling_Detail[0]?.versionNum ?? string.Empty;
                //获取当前的VersionNumber
                string versionNumber = Year.ToString() + round.Substring(0, 2);

                //获取上一回目的VersionNumber
                int versionAsNumber;
                string lastVersionNumber;
                try
                {
                    versionAsNumber = int.Parse(versionNumber) - 1;
                    lastVersionNumber = versionAsNumber.ToString();
                }
                catch (Exception ex)
                {
                    log.LogError($"【请求服务器有误】{ex.Message}");
                    return new BadRequestObjectResult(new VersionControlResponse
                    {
                        Status = StatusCodes.Status400BadRequest.ToString(),
                        Message = "受信バージョンが間違っていました"// 传入的版本有误
                    });
                }
                //存储前端传入的型番
                List<string> psi_allModelsList = new List<string>();
                foreach (var entity in psi_allModels)
                {

                    psi_allModelsList.Add(entity.Value);
                }


                #endregion

                #region 查询Xml
                // 将型番作为查询条件并转换为FetchXML查询商品表
                string psi_allModelsCode_fetch = $"<condition attribute='sfa_name' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>") )}</condition>";
                // 将型番作为查询条件并转换为FetchXML查询滚动预算详细
                string psi_allModelsCode_fetch_RollingDeatil = $"<condition attribute='sfa_p_name' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                // 将型番作为查询条件并转换为FetchXML查询预算详细
                string psi_allModelsCode_fetch_BudgetDeatil = $"<condition attribute='sfa_modelname' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                // 将型番作为查询条件并转换为FetchXML查询PSI详细
                string psi_allModelsCode_AdjustDeatil_fetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("",psi_allModelsList.Select(item => $"<value>{item}</value>"))}</condition>";
                //将登录番号作为查询PSI详细表的条件
                string psi_applicationNumber_fetchxml = $"<condition attribute='sfa_title' operator='eq' value='{mainRollingBudgetId}'/>";
                //查询12个月的xml
                string psi_Month_fetch = string.Join("\r\n", Array.ConvertAll(month, item => $"<condition attribute='sfa_month' operator='eq' value='{item}'/>"));
                //查询当前登录番号的XML
                string mainRollingIdXml = $"<condition attribute='sfa_sn' operator='eq' value='{mainRollingBudgetId}'/>";
                string main_PSI_IdXml = $"<condition attribute='sfa_title' operator='eq' value='{mainRollingBudgetId}'/>";
                //查询当前年度和回目的XML
                string versionNumberXml = $"<condition attribute='sfa_versionnumber' operator='eq' value='{versionNumber}'/>";
                //查询当前年度上一回目的XML
                string lastVersionNumberXml = $"<condition attribute='sfa_versionnumber' operator='eq' value='{lastVersionNumber}'/>";
                //查询当前年的Xml
                string yearXml = $"<condition attribute='sfa_year' operator='eq' value='{Year}'/>";
                //查询去年的Xml
                string lastYearXml = $"<condition attribute='sfa_year' operator='eq' value='{Year - 1}'/>";
                #endregion

                //从商品表中获取到全部型番的SAPcode和Keycode
                EntityCollection psi_allModels_keycode = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModels", psi_allModelsCode_fetch), serviceClient);

                //从法人master表中获取到所有PSI调整为True的法人
                EntityCollection entity_Psi_LegalCollection = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Legal_Master("BR_PSI_Legal"), serviceClient);

                //存储PSI法人为True的name和code
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

                //将PSI为true的法人作为查询滚动予算的条件转换为FetchXML
                string sfa_fetch_psi_legalName = $"<condition attribute='sfa_c_name' operator='in'>{string.Join("", sfa_legal_nameList.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                //将PSI为true的法人作为查询导入管理的条件转换为FetchXML
                string sfa_fetch_legalName = string.Join("\r\n", sfa_legal_nameList.ConvertAll(item => $"<condition attribute='sfa_name' operator='eq' value='{item}' />"));
                //将PSI为true的法人code作为查询实绩和受损表的条件转换为FetchXML
                string sfa_fetch_psi_legalcode_xml = string.Join("\r\n", sfa_legal_codeList.ConvertAll(item => $"<condition attribute='sfa_c_sapcode' operator='eq' value='{item}' />"));
                //从滚动予算表中获取型番，型番对应的法人以及对应的SAPcode
                EntityCollection psi_ModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumberAndLegal", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml), serviceClient);
                //从导入管理表中获取型番和PSI对象法人的组合
                EntityCollection ModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI("B001", yearXml, sfa_fetch_legalName, psi_allModelsCode_fetch), serviceClient);
                //存储PSI型番和型番对应的法人
                var psi_all_ModelsAndlegal = ModelsAndLegal.Entities
     .Where(entity => entity.Contains("sfa_kunnr") && entity.Contains("sfa_zcusmodel"))
     .Select(entity => new
     {
         sfa_kunnr = entity.Contains("sfa_kunnr") ? ((EntityReference)entity["sfa_kunnr"]).Name : null,  // 获取 sfa_kunnr 的 Name
         sfa_kunnr_code = entity.Contains("EMP1.sfa_sapcode") ? entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode")?.Value : null,  // 安全访问 EMP1.sfa_sapcode
         sfa_zcusmodel = entity.Contains("sfa_zcusmodel") ? ((EntityReference)entity["sfa_zcusmodel"]).Name : null,  // 获取 sfa_zcusmodel 的 Name
         sfa_zcusmodel_code = entity.Contains("EMP2.sfa_sapcode") ? entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode")?.Value : null  // 安全访问 EMP2.sfa_sapcode
     })
     .ToList();


                //获取型番的SAPcode并存储到List集合中
                List<string> sfa_p_code_list = new List<string>();
                foreach (var entity in ModelsAndLegal.Entities)
                {
                    if (entity.Contains("EMP1.sfa_sapcode") && entity["EMP1.sfa_sapcode"] != null)
                    {
                        sfa_p_code_list.Add(entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode").Value.ToString());
                    }
                }

                //获取法人的SAPcode并存储到List集合中
                List<string> sfa_c_code_list = new List<string>();
                foreach (var entity in ModelsAndLegal.Entities)
                {
                    if (entity.Contains("EMP2.sfa_sapcode") && entity["EMP2.sfa_sapcode"] != null)
                    {
                        sfa_c_code_list.Add(entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode").Value.ToString());
                    }
                }

                //将型番的SAPcode作为查询AI予测的存储条件转换为FetchXML
                string sfa_fetch_p_codeXml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("", sfa_p_code_list.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                //将法人的SAPcode作为查询AI予测的存储条件转换为FetchXML
                string sfa_fetch_c_codeXml = $"<condition attribute='sfa_c_sapcode' operator='in'>{string.Join("", sfa_c_code_list.ConvertAll(item => $"<value>{item}</value>"))}</condition>";

                // 从 Dataverse 表结构生成 DataTable
                string entityLogicalName = "sfa_psi_details"; // 替换为目标表逻辑名称
                DataTable dt = CreateDataTableFromDataverseSchema(entityLogicalName, serviceClient);
                //根据登录番号判断PSI详细表中是否存在对应的记录
                EntityCollection entityCollection_record = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_PSI_Deatils", main_PSI_IdXml, "")));
                if (entityCollection_record.Entities.Count == 0)
                {
                    #region 调整数量
                    //根据登录番号，型番，月份从PSI詳細表中查询
                    EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", psi_allModelsCode_AdjustDeatil_fetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                    //存储12个月对应型番的调整数量的List
                    List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                    foreach (var model in psi_allModelsList)
                    {
                        for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                        {
                            bool flag = true;
                            foreach (var adjust_record in psi_adjust_num.Entities)
                            {
                                //比较记录中的型番，月份，以及是否存在sfa_adjusted_quantities属性
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

                    #region 今回合計,コメント
                    //根据versionNumber和PSI法人获取当前回目十二个月份传入型番的记录
                    EntityCollection psi_Jan_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml)));
                    //存储每个型番12个月的数量
                    List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                    //存储每个型番12个月的コメント
                    List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                    //遍历前端传入型番和法人的全部组合
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                    //根据型番和月份将今回数量相加
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

                    #region 前回合計
                    //根据versionNumber和PSI法人获取上一回目十二个月份传入型番的记录
                    EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                    //存储上一回目每个型番12个月的数量
                    List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                    //遍历前端传入型番和法人的全部组合
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                    #region 予算
                    //获取予算最新版本
                    EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                    //存储每个型番12个月的予算数量
                    List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                    //最新-版本号
                    string versionnumber = "";
                    if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                    {
                        var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                        //最新-版本号
                        versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                    }
                    else
                    {
                        return new BadRequestObjectResult(new VersionControlResponse
                        {
                            Status = StatusCodes.Status500InternalServerError.ToString(),
                            Message = "選択した法人には有効型番が存在していません、導入管理を確認してください"
                        });
                    }

                    //将PSI为true的法人作为查询予算明细的存储条件转换为FetchXML
                    string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.ConvertAll(item => $"<value>{item}</value>"))}</condition>";
                    //查寻当前版本num的xml
                    string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                    //获取予算详细
                    EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (result_Budget_BVersion.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = result_Budget_BVersion.Entities
                .Where(entity =>
                    entity.Contains("sfa_modelname") &&
                    entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_legalname") &&
                    entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                    //根据型番和月份将予算数量相加
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

                    #region 直送前半
                    //存储当前回目每个型番12个月直送前半的数量
                    List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                    #region 直送后半
                    //存储当前回目每个型番12个月直送前半的数量
                    List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_ModelsAndLegal.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_ModelsAndLegal.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                    #region 前回直送前半
                    //存储上一回目每个型番12个月直送前半的数量
                    List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                    //根据型番和月份将直送前半相加
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

                    #region 前回直送后半
                    //存储上一回目每个型番12个月直送后半的数量
                    List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                    //按照型番分组

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;
                            //比较滚动予算表中存在的记录
                            if (psi_last_ModelsNum.Entities.Count != 0)
                            {
                                //获取有效的型番和法人组合
                                var validCombination = psi_last_ModelsNum.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_name") &&
                    entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                    entity.Contains("sfa_c_name") &&
                    entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                    //根据型番和月份将直送后半相加
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

                    #region AI予測
                    EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                    //存储每个型番12个月的AI予测的数量
                    List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                    foreach (var model_legal in psi_all_ModelsAndlegal)
                    {
                        //m代表12个月份
                        for (int m = 1; m <= 12; m++)
                        {
                            bool flag = false;

                            if (psi_AINumber.Entities.Count != 0)
                            {
                                //获取有效的型番code和法人code组合
                                var validCombination = psi_AINumber.Entities
                .Where(entity =>
                    entity.Contains("sfa_p_sapcode") &&
                    entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // 匹配 sfa_zcusmodel_code
                    entity.Contains("sfa_c_sapcode") &&
                    entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // 匹配 sfa_kunnr_code
                .ToList();
                                //如果有符合进行对应记录的添加
                                if (validCombination.Any())
                                {
                                    // 遍历有效的组合，进行后续操作
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
                                    //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                    //根据型番和月份将AI数量相加
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

                    #region 実績,実売,粗利率
                    //定义存放粗利结果的列表
                    List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                    // 定义一个存放実売结果的列表
                    List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                    // 定义一个存放実績结果的列表
                    List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                    //根据年度，月份，型番的SAPcode和法人的SAPcode拿到売上実績テーブル表中对应的记录
                    EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                    //根据年度，月份，型番的SAPcode和法人的SAPcode拿到受损表中对应的记录
                    EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                    // 按月份和sap_p_code进行分组
                    var achi_monthlySums = psi_achi_number.Entities
                        .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                        .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                        .ToDictionary(
                            group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                            group => new
                            {
                                QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // 计算sfa_grossprofit总和
                            }
                        );
                    // 按月份和sap_p_code进行分组
                    var des_monthlySums = psi_Des_number.Entities
                        .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                        .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                        .ToDictionary(
                            group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                            group => new
                            {
                                QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // 计算sfa_grossprofit总和
                            }
                        );



                    // 将型番codeList转换为HashSetList
                    HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                    foreach (var scode in sfa_p_code_hashSet)
                    {
                        // 计算実績，毛利，実売的值
                        for (int m = 1; m <= 12; m++)
                        {
                            // 获取achi和des中对应月份的数量
                            string p_code = "";
                            decimal achiQuantity = 0;
                            decimal desQuantity = 0;
                            decimal achiGrossProfit = 0, desGrossProfit = 0;
                            p_code = scode;
                            // 如果achi_monthlySums中存在该月份的数据，则获取数量和毛利
                            if (achi_monthlySums.ContainsKey((m, scode)))
                            {
                                achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                            }

                            // 如果des_monthlySums中存在该月份的数据，则获取数量
                            if (des_monthlySums.ContainsKey((m, scode)))
                            {
                                desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                            }
                            // 将数量相加
                            decimal totalQuantity = achiQuantity + desQuantity;
                            //将毛利相加
                            decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                            //実売值
                            decimal totalAchiQuantity = achiQuantity;
                            // 创建一个ModelMonthNum对象并将其加入结果列表
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

                    #region 同期

                    //遍历获取到型番的KeyCode集合
                    HashSet<string> models_kecode_list = new HashSet<string>();
                    List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                    foreach (var kecode_record in psi_allModels_keycode.Entities)
                    {
                        if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                        {
                            models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                        }
                    }

                    //根据获取到的KeyCode拼接为XML，根据KeyCodeXML获取商品表中对应的型番的Sapcode
                    string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                    //根据KeycdoeXML获取商品表中对应的型番的Sapcode
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

                    //从 psi_keycode_allModels中提取型番的sfa_sapcode并拼接成查询实绩和受损表中的XML 条件
                    string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("",
     psi_keycode_allLegal.Entities
         .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // 过滤包含 sfa_sapcode 属性的实体
         .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>")
 )}</condition>";

                    //根据allModel_kecode_fetchxml和sfa_fetch_psi_legalcode_xml到实绩表和受损表中查询去年对应的数量
                    EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                    EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                    // 根据SapCode和月份分组并计算实绩和受损数量总和
                    var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                        .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                        .GroupBy(entity => new
                        {
                            sapcode = (string)entity["sfa_p_sapcode"],
                            Month = (int)entity["sfa_month"]
                        }) // 按 Keycode 和月份分组
                        .ToDictionary(
                            group => group.Key, // 分组的键为 Keycode 和 Month
                            group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                        );
                    var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                        .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                        .GroupBy(entity => new
                        {
                            sapcode = (string)entity["sfa_p_sapcode"],
                            Month = (int)entity["sfa_month"]
                        }) // 按 Keycode 和月份分组
                        .ToDictionary(
                            group => group.Key, // 分组的键为 Keycode 和 Month
                            group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                        );

                    //将achiGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                    foreach (var model in models_kecode)
                    {
                        var key = new
                        {
                            sapcode = model.Model, // Model 对应 sapcode
                            Month = model.Month
                        };

                        if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                        {
                            model.ModelSum = value; // 赋值对应的总和
                        }
                    }

                    //将desGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                    foreach (var model in models_kecode)
                    {
                        var key = new
                        {
                            sapcode = model.Model, // Model 对应 sapcode
                            Month = model.Month
                        };

                        if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                        {
                            model.ModelSum += value; // 将值相加
                        }
                    }
                    var mergedModelsKecode = models_kecode
        .GroupBy(item => new { item.Keycode, item.Month }) // 按 Keycode 和 Month 分组
        .Select(group => new ModelMonthNum
        {
            Keycode = group.Key.Keycode,
            Month = group.Key.Month,
            Model = string.Join(";", group.Select(x => x.Model)), // 合并 Model 值（如需要）
            ModelSum = group.Sum(x => x.ModelSum) // 汇总 ModelSum
        })
        .ToList();

                    #endregion

                    //存入大宽表中
                    foreach (var item in round_Number)
                    {
                        // 创建一行数据
                        DataRow newRow = dt.NewRow();
                        #region 今回合计
                        // 为 sfa_month 列赋值，月份
                        newRow["sfa_month"] = item.Month; // 赋值月份

                        // 为 sfa_p_names 列赋值，Model
                        newRow["sfa_p_names"] = item.Model; // 赋值 Model

                        // 为 sfa_all_sum_number 列赋值，ModelSum（来自 round_Number）
                        newRow["sfa_all_sum_number"] = item.ModelSum; // 赋值 round_Number 的 ModelSum
                        #endregion
                        //根据型番的item.Model的值与psi_ModelsAndLegal的sfa_p_code的值进行匹配
                        foreach (var pname in psi_ModelsAndLegal.Entities)
                        {
                            if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                            {
                                newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // 赋值 direct_beforeNumber 的 ModelSum
                                newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // 赋值 direct_beforeNumber 的 ModelSum
                                break;
                            }

                        }
                        #region 前回合计
                        // 查找对应的 lastRound_Number 数据
                        var lastRound = lastRound_Number
                                        .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                        if (lastRound != null)
                        {
                            newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            newRow["sfa_allsumnumber_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 前回差
                        // 计算前回差（今回合計 - 前回合計）
                        var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                        // 为 sfa_num_balance 列赋值，前回差（今回合計 - 前回合計）
                        newRow["sfa_num_balance"] = previousDiff;
                        #endregion
                        #region 予算
                        // 查找对应的预算数据
                        var budgetDetail = eachModel_BugetNumber
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的预算数据，则为预算列赋值
                        if (budgetDetail != null)
                        {
                            newRow["sfa_budget_num"] = budgetDetail.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的预算数据，设置为0或者其他默认值
                            newRow["sfa_budget_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 同期
                        // 查找对应的同期数据
                        var samePeriod_num = mergedModelsKecode
                                            .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                        // 如果找到对应的预算数据，则为预算列赋值
                        if (samePeriod_num != null)
                        {
                            newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的预算数据，设置为0或者其他默认值
                            newRow["sfa_sameperiod_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 直送前半
                        // 查找对应的直送前半数据
                        var direct_beforeNumber = round_direct_beforeNum
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送前半数据，则为直送前半列赋值
                        if (direct_beforeNumber != null)
                        {
                            newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                            newRow["sfa_direct_before"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 直送后半
                        // 查找对应的直送后半数据
                        var direct_afterNumber = round_direct_afterNum
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送后半数据，则为直送后半列赋值
                        if (direct_afterNumber != null)
                        {
                            newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                            newRow["sfa_direct_after"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 前回直送前半
                        // 查找对应的直送前半数据
                        var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送前半数据，则为直送前半列赋值
                        if (lastRound_direct_beforeNumber != null)
                        {
                            newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                            newRow["sfa_direct_before_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 前回直送後半
                        // 查找对应的直送后半数据
                        var last_direct_afterNumber = lastRound_direct_after_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送后半数据，则为直送后半列赋值
                        if (last_direct_afterNumber != null)
                        {
                            newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                            newRow["sfa_direct_after_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 直送前半差
                        //计算直送前半差(直送前半-前回直送前半)
                        var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                        // 为 sfa_direct_before_balance 列赋值，前回差（今回合計 - 前回合計）
                        newRow["sfa_direct_before_balance"] = before_half_dif;
                        #endregion
                        #region 直送後半差
                        //计算直送後半差(直送後半-前回直送後半)
                        var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                        // 为 sfa_direct_after_balance 列赋值，前回差（今回合計 - 前回合計）
                        newRow["sfa_direct_after_balance"] = after_half_dif;
                        #endregion
                        #region AI予測
                        // 查找对应的AI予测数据
                        var AI_number = round_AINumber
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送前半数据，则为直送前半列赋值
                        if (AI_number != null)
                        {
                            newRow["sfa_ai_num"] = AI_number.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                            newRow["sfa_ai_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 実績
                        var achi_des_record = achi_des_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                        if (achi_des_record != null)
                        {
                            newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            newRow["sfa_sales_perf_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 実売
                        var achi_record = achi_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                        if (achi_record != null)
                        {
                            newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            newRow["sfa_sales_retail_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 粗利率
                        var gro_record = grossProfit_num
                                        .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                        // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                        if (gro_record != null && achi_des_record.ModelSum != 0)
                        {
                            newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            newRow["sfa_perf_grossprofit"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 進捗率
                        //実績/前回合計
                        if (achi_des_record != null && lastRound.ModelSum != 0)
                        {
                            newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                        }
                        else
                        {
                            newRow["sfa_progressrate"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region コメント
                        var model_reason = all_ModelReasonList
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                        // 如果找到对应的直送前半数据，则为直送前半列赋值
                        if (model_reason != null)
                        {
                            newRow["sfa_num_reason"] = model_reason.Reason; // 赋值 direct_beforeNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的评论数据，设置为0或者其他默认值
                            newRow["sfa_num_reason"] = ""; // 如果没有找到对应数据，则赋值为 "" 或者其他默认值
                        }
                        #endregion
                        #region 调整数量
                        var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                        if (Adjust_num != null)
                        {
                            newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                        }
                        else
                        {
                            // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                            newRow["sfa_adjusted_quantities"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                        }
                        #endregion
                        #region 出荷比率

                        #endregion
                        // 将新行添加到 DataTable
                        dt.Rows.Add(newRow);
                    }
                }
                else
                {
                    //PSI详细表中具有该型番记录的直接拿取数据，如果没有则和if同逻辑去取对应型番对应的数量
                    var psiDetails_ModelsAndlegal = entityCollection_record.Entities
    .Where(entity => entity.Contains("sfa_title") && entity.Contains("sfa_p_names"))
    .Select(entity => new
    {
        sfa_kunnr = "PSI",
        sfa_zcusmodel = entity["sfa_p_names"].ToString()  // 获取 sfa_zcusmodel 的 Name
    })
    .Distinct()
    .ToList();
                    #region 没有与前端匹配的型番
                    // 创建一个集合存储 psiDetails_ModelsAndlegal 中的 sfa_zcusmodel 值
                    var psiDetailsModelsSet = new HashSet<string>(
                        psiDetails_ModelsAndlegal.Select(item => item.sfa_zcusmodel)
                    );

                    // 获取 psi_allModelsList 中与 psiDetailsModelsSet 相同的项
                    var commonItems = psi_allModelsList
                        .Where(model => psiDetailsModelsSet.Contains(model))
                        .Distinct()
                        .ToList();
                    // 获取 psi_allModelsList 中与 psiDetailsModelsSet 不同的项
                    var differentItems = psi_allModelsList
                        .Where(model => !psiDetailsModelsSet.Contains(model))
                        .ToList();
                    //没有相同项，则正常从各个表中获取数量
                    if (commonItems.Count == 0)
                    {
                        #region 调整数量
                        //根据登录番号，型番，月份从PSI詳細表中查询
                        EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", psi_allModelsCode_AdjustDeatil_fetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                        //存储12个月对应型番的调整数量的List
                        List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                        foreach (var model in psi_allModelsList)
                        {
                            for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                            {
                                bool flag = true;
                                foreach (var adjust_record in psi_adjust_num.Entities)
                                {
                                    //比较记录中的型番，月份，以及是否存在sfa_adjusted_quantities属性
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

                        #region 今回合計,コメント
                        //根据versionNumber和PSI法人获取当前回目十二个月份传入型番的记录
                        EntityCollection psi_Jan_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, versionNumberXml)));
                        //存储每个型番12个月的数量
                        List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                        //存储每个型番12个月的コメント
                        List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                        //遍历前端传入型番和法人的全部组合
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将今回数量相加
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

                        #region 前回合計
                        //根据versionNumber和PSI法人获取上一回目十二个月份传入型番的记录
                        EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                        //存储上一回目每个型番12个月的数量
                        List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                        //遍历前端传入型番和法人的全部组合
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 予算
                        //获取予算最新版本
                        EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                        //存储每个型番12个月的予算数量
                        List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                        //最新-版本号
                        string versionnumber = "";
                        if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                        {
                            var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                            //最新-版本号
                            versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                        }
                        else
                        {
                            return new BadRequestObjectResult(new VersionControlResponse
                            {
                                Status = StatusCodes.Status500InternalServerError.ToString(),
                                Message = "選択した法人には有効型番が存在していません、導入管理を確認してください"
                            });
                        }

                        //将PSI为true的法人作为查询予算明细的存储条件转换为FetchXML
                        string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.Select(item => $"<value>{item}</value>")
)}</condition>";
                        //查寻当前版本num的xml
                        string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                        //获取予算详细
                        EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (result_Budget_BVersion.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = result_Budget_BVersion.Entities
                    .Where(entity =>
                        entity.Contains("sfa_modelname") &&
                        entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_legalname") &&
                        entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将予算数量相加
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

                        #region 直送前半
                        //存储当前回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 直送后半
                        //存储当前回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 前回直送前半
                        //存储上一回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将直送前半相加
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

                        #region 前回直送后半
                        //存储上一回目每个型番12个月直送后半的数量
                        List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                        //按照型番分组

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将直送后半相加
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

                        #region AI予測
                        EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //存储每个型番12个月的AI予测的数量
                        List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_all_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;

                                if (psi_AINumber.Entities.Count != 0)
                                {
                                    //获取有效的型番code和法人code组合
                                    var validCombination = psi_AINumber.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_sapcode") &&
                        entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // 匹配 sfa_zcusmodel_code
                        entity.Contains("sfa_c_sapcode") &&
                        entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // 匹配 sfa_kunnr_code
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将AI数量相加
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

                        #region 実績,実売,粗利率
                        //定义存放粗利结果的列表
                        List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                        // 定义一个存放実売结果的列表
                        List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                        // 定义一个存放実績结果的列表
                        List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                        //根据年度，月份，型番的SAPcode和法人的SAPcode拿到売上実績テーブル表中对应的记录
                        EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //根据年度，月份，型番的SAPcode和法人的SAPcode拿到受损表中对应的记录
                        EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                        // 按月份和sap_p_code进行分组
                        var achi_monthlySums = psi_achi_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                    GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // 计算sfa_grossprofit总和
                                }
                            );
                        // 按月份和sap_p_code进行分组
                        var des_monthlySums = psi_Des_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                    GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // 计算sfa_grossprofit总和
                                }
                            );



                        // 将型番codeList转换为HashSetList
                        HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                        foreach (var scode in sfa_p_code_hashSet)
                        {
                            // 计算実績，毛利，実売的值
                            for (int m = 1; m <= 12; m++)
                            {
                                // 获取achi和des中对应月份的数量
                                string p_code = "";
                                decimal achiQuantity = 0;
                                decimal desQuantity = 0;
                                decimal achiGrossProfit = 0, desGrossProfit = 0;
                                p_code = scode;
                                // 如果achi_monthlySums中存在该月份的数据，则获取数量和毛利
                                if (achi_monthlySums.ContainsKey((m, scode)))
                                {
                                    achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                    achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                                }

                                // 如果des_monthlySums中存在该月份的数据，则获取数量
                                if (des_monthlySums.ContainsKey((m, scode)))
                                {
                                    desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                    desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                                }
                                // 将数量相加
                                decimal totalQuantity = achiQuantity + desQuantity;
                                //将毛利相加
                                decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                                //実売值
                                decimal totalAchiQuantity = achiQuantity;
                                // 创建一个ModelMonthNum对象并将其加入结果列表
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

                        #region 同期

                        //遍历获取到型番的KeyCode集合
                        HashSet<string> models_kecode_list = new HashSet<string>();
                        List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                        foreach (var kecode_record in psi_allModels_keycode.Entities)
                        {
                            if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                            {
                                models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                            }
                        }

                        //根据获取到的KeyCode拼接为XML，根据KeyCodeXML获取商品表中对应的型番的Sapcode
                        string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                        //根据KeycdoeXML获取商品表中对应的型番的Sapcode
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

                        //从 psi_keycode_allModels中提取型番的sfa_sapcode并拼接成查询实绩和受损表中的XML 条件
                        string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("",
    psi_keycode_allLegal.Entities
        .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // 检查是否包含 sfa_sapcode 属性
        .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>")
)}</condition>";

                        //根据allModel_kecode_fetchxml和sfa_fetch_psi_legalcode_xml到实绩表和受损表中查询去年对应的数量
                        EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        // 根据SapCode和月份分组并计算实绩和受损数量总和
                        var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // 按 Keycode 和月份分组
                            .ToDictionary(
                                group => group.Key, // 分组的键为 Keycode 和 Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                            );
                        var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // 按 Keycode 和月份分组
                            .ToDictionary(
                                group => group.Key, // 分组的键为 Keycode 和 Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                            );

                        //将achiGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model 对应 sapcode
                                Month = model.Month
                            };

                            if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum = value; // 赋值对应的总和
                            }
                        }

                        //将desGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model 对应 sapcode
                                Month = model.Month
                            };

                            if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum += value; // 将值相加
                            }
                        }
                        var mergedModelsKecode = models_kecode
            .GroupBy(item => new { item.Keycode, item.Month }) // 按 Keycode 和 Month 分组
            .Select(group => new ModelMonthNum
            {
                Keycode = group.Key.Keycode,
                Month = group.Key.Month,
                Model = string.Join(";", group.Select(x => x.Model)), // 合并 Model 值（如需要）
                ModelSum = group.Sum(x => x.ModelSum) // 汇总 ModelSum
            })
            .ToList();

                        #endregion

                        //存入大宽表中
                        foreach (var item in round_Number)
                        {
                            // 创建一行数据
                            DataRow newRow = dt.NewRow();
                            #region 今回合计
                            // 为 sfa_month 列赋值，月份
                            newRow["sfa_month"] = item.Month; // 赋值月份

                            // 为 sfa_p_names 列赋值，Model
                            newRow["sfa_p_names"] = item.Model; // 赋值 Model

                            // 为 sfa_all_sum_number 列赋值，ModelSum（来自 round_Number）
                            newRow["sfa_all_sum_number"] = item.ModelSum; // 赋值 round_Number 的 ModelSum
                            #endregion
                            //根据型番的item.Model的值与psi_ModelsAndLegal的sfa_p_code的值进行匹配
                            foreach (var pname in psi_ModelsAndLegal.Entities)
                            {
                                if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                                {
                                    newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // 赋值 direct_beforeNumber 的 ModelSum
                                    newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // 赋值 direct_beforeNumber 的 ModelSum
                                    break;
                                }

                            }
                            #region 前回合计
                            // 查找对应的 lastRound_Number 数据
                            var lastRound = lastRound_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (lastRound != null)
                            {
                                newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_allsumnumber_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回差
                            // 计算前回差（今回合計 - 前回合計）
                            var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                            // 为 sfa_num_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_num_balance"] = previousDiff;
                            #endregion
                            #region 予算
                            // 查找对应的预算数据
                            var budgetDetail = eachModel_BugetNumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的预算数据，则为预算列赋值
                            if (budgetDetail != null)
                            {
                                newRow["sfa_budget_num"] = budgetDetail.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的预算数据，设置为0或者其他默认值
                                newRow["sfa_budget_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 同期
                            // 查找对应的同期数据
                            var samePeriod_num = mergedModelsKecode
                                                .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                            // 如果找到对应的预算数据，则为预算列赋值
                            if (samePeriod_num != null)
                            {
                                newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的预算数据，设置为0或者其他默认值
                                newRow["sfa_sameperiod_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送前半
                            // 查找对应的直送前半数据
                            var direct_beforeNumber = round_direct_beforeNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_direct_before"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送后半
                            // 查找对应的直送后半数据
                            var direct_afterNumber = round_direct_afterNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送后半数据，则为直送后半列赋值
                            if (direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                                newRow["sfa_direct_after"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回直送前半
                            // 查找对应的直送前半数据
                            var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (lastRound_direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_direct_before_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回直送後半
                            // 查找对应的直送后半数据
                            var last_direct_afterNumber = lastRound_direct_after_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送后半数据，则为直送后半列赋值
                            if (last_direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                                newRow["sfa_direct_after_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送前半差
                            //计算直送前半差(直送前半-前回直送前半)
                            var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                            // 为 sfa_direct_before_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_direct_before_balance"] = before_half_dif;
                            #endregion
                            #region 直送後半差
                            //计算直送後半差(直送後半-前回直送後半)
                            var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                            // 为 sfa_direct_after_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_direct_after_balance"] = after_half_dif;
                            #endregion
                            #region AI予測
                            // 查找对应的AI予测数据
                            var AI_number = round_AINumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (AI_number != null)
                            {
                                newRow["sfa_ai_num"] = AI_number.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_ai_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 実績
                            var achi_des_record = achi_des_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (achi_des_record != null)
                            {
                                newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_perf_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 実売
                            var achi_record = achi_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (achi_record != null)
                            {
                                newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_retail_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 粗利率
                            var gro_record = grossProfit_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (gro_record != null && achi_des_record.ModelSum != 0)
                            {
                                newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_perf_grossprofit"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 進捗率
                            //実績/前回合計
                            if (achi_des_record != null && lastRound.ModelSum != 0)
                            {
                                newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_progressrate"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region コメント
                            var model_reason = all_ModelReasonList
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (model_reason != null)
                            {
                                newRow["sfa_num_reason"] = model_reason.Reason; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的评论数据，设置为0或者其他默认值
                                newRow["sfa_num_reason"] = ""; // 如果没有找到对应数据，则赋值为 "" 或者其他默认值
                            }
                            #endregion
                            #region 调整数量
                            var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                            if (Adjust_num != null)
                            {
                                newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_adjusted_quantities"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 出荷比率

                            #endregion
                            // 将新行添加到 DataTable
                            dt.Rows.Add(newRow);
                        }
                    }
                    #endregion
                    #region 拿到与前端传入型番相匹配的数据
                    else //如果有相同项
                    {
                        //相同项的值从PSI详细表中拿取，不相同的项从各个表中获取
                        //将相同项的型番内容转成查询PSI详细表的xml
                        string PSI_Details_Models_FetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("", commonItems.Select(item => $"<value>{item}</value>"))}</condition>";

                        //查询共同型番的PSI详细表中的记录
                        EntityCollection entityCollection_PSI_DetailsCommonRecord = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_PSI_Deatils", main_PSI_IdXml, PSI_Details_Models_FetchXml)));
                        #region 处理匹配相同的型番
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
                                        // 为 sfa_month 列赋值，月份
                                        newRow["sfa_month"] = m; // 赋值月份
                                        // 为 sfa_p_names 列赋值，Model
                                        newRow["sfa_p_names"] = common_record["sfa_p_names"].ToString();// 赋值 Model
                                        //型番code
                                        newRow["sfa_p_sapcode"] = common_record.Contains("sfa_p_sapcode") ? common_record["sfa_p_sapcode"].ToString() : "";
                                        //法人name
                                        newRow["sfa_c_names"] = common_record["sfa_c_names"].ToString();
                                        // 今回合计
                                        newRow["sfa_all_sum_number"] = Convert.ToDecimal(common_record["sfa_all_sum_number"]);
                                        //前回合计                                                                   
                                        newRow["sfa_allsumnumber_pre"] = Convert.ToDecimal(common_record["sfa_allsumnumber_pre"]);
                                        //前回差
                                        newRow["sfa_num_balance"] = Convert.ToDecimal(common_record["sfa_num_balance"]);
                                        //予算
                                        newRow["sfa_budget_num"] = Convert.ToDecimal(common_record["sfa_budget_num"]);
                                        //同期
                                        newRow["sfa_sameperiod_num"] = Convert.ToDecimal(common_record["sfa_sameperiod_num"]);
                                        //直送前半
                                        newRow["sfa_direct_before"] = Convert.ToDecimal(common_record["sfa_direct_before"]);
                                        //直送後半
                                        newRow["sfa_direct_after"] = Convert.ToDecimal(common_record["sfa_direct_after"]);
                                        //前回直送前半
                                        newRow["sfa_direct_before_pre"] = Convert.ToDecimal(common_record["sfa_direct_before_pre"]);
                                        //前回直送後半
                                        newRow["sfa_direct_after_pre"] = Convert.ToDecimal(common_record["sfa_direct_after_pre"]);
                                        //直送前半差
                                        newRow["sfa_direct_before_balance"] = Convert.ToDecimal(common_record["sfa_direct_before_balance"]);
                                        //直送後半差
                                        newRow["sfa_direct_after_balance"] = Convert.ToDecimal(common_record["sfa_direct_after_balance"]);
                                        //AI予測
                                        newRow["sfa_ai_num"] = Convert.ToDecimal(common_record["sfa_ai_num"]);
                                        //実績
                                        newRow["sfa_sales_perf_num"] = Convert.ToDecimal(common_record["sfa_sales_perf_num"]);
                                        //実売
                                        newRow["sfa_sales_retail_num"] = Convert.ToDecimal(common_record["sfa_sales_retail_num"]);
                                        //粗利率
                                        newRow["sfa_perf_grossprofit"] = Convert.ToDecimal(common_record["sfa_perf_grossprofit"]);
                                        //進捗率
                                        newRow["sfa_progressrate"] = Convert.ToDecimal(common_record["sfa_progressrate"]);
                                        //コメント
                                        newRow["sfa_num_reason"] = common_record.Contains("sfa_num_reason") ? common_record["sfa_num_reason"].ToString() : "";
                                        //调整数量
                                        newRow["sfa_adjusted_quantities"] = Convert.ToDecimal(common_record["sfa_adjusted_quantities"]);
                                        dt.Rows.Add(newRow);
                                        flag = true;
                                    }
                                    //为缺失月份赋值
                                    if (!flag)
                                    {
                                        DataRow newRow = dt.NewRow();
                                        // 为 sfa_month 列赋值，月份
                                        newRow["sfa_month"] = m; // 赋值月份
                                        // 为 sfa_p_names 列赋值，Model
                                        newRow["sfa_p_names"] = commonModel;// 赋值 Model
                                        //型番code
                                        newRow["sfa_p_sapcode"] = entityCollection_PSI_DetailsCommonRecord.Entities
    .Where(e => e.Contains("sfa_p_names") && e["sfa_p_names"] != null && e["sfa_p_names"].ToString() == commonModel)
    .Select(e => e.Contains("sfa_p_code") && e["sfa_p_code"] != null ? e["sfa_p_code"].ToString() : null)
    .FirstOrDefault();
                                        //法人name
                                        newRow["sfa_c_names"] = "PSI";
                                        // 今回合计0
                                        newRow["sfa_all_sum_number"] = Convert.ToDecimal(0);
                                        //前回合计                                                                   
                                        newRow["sfa_allsumnumber_pre"] = Convert.ToDecimal(0);
                                        //前回差
                                        newRow["sfa_num_balance"] = Convert.ToDecimal(0);
                                        //予算
                                        newRow["sfa_budget_num"] = Convert.ToDecimal(0);
                                        //同期
                                        newRow["sfa_sameperiod_num"] = Convert.ToDecimal(0);
                                        //直送前半
                                        newRow["sfa_direct_before"] = Convert.ToDecimal(0);
                                        //直送後半
                                        newRow["sfa_direct_after"] = Convert.ToDecimal(0);
                                        //前回直送前半
                                        newRow["sfa_direct_before_pre"] = Convert.ToDecimal(0);
                                        //前回直送後半
                                        newRow["sfa_direct_after_pre"] = Convert.ToDecimal(0);
                                        //直送前半差
                                        newRow["sfa_direct_before_balance"] = Convert.ToDecimal(0);
                                        //直送後半差
                                        newRow["sfa_direct_after_balance"] = Convert.ToDecimal(0);
                                        //AI予測
                                        newRow["sfa_ai_num"] = Convert.ToDecimal(0);
                                        //実績
                                        newRow["sfa_sales_perf_num"] = Convert.ToDecimal(0);
                                        //実売
                                        newRow["sfa_sales_retail_num"] = Convert.ToDecimal(0);
                                        //粗利率
                                        newRow["sfa_perf_grossprofit"] = Convert.ToDecimal(0);
                                        //進捗率
                                        newRow["sfa_progressrate"] = Convert.ToDecimal(0);
                                        //コメント
                                        newRow["sfa_num_reason"] = "";
                                        //调整数量
                                        newRow["sfa_adjusted_quantities"] = Convert.ToDecimal(0);
                                        dt.Rows.Add(newRow);
                                    }
                                }
                            }
                        }
                        #endregion
                        #region 处理匹配不同的型番
                        //将不同项的型番内容转成查询导入管理表的xml
                        string PSI_Diff_Models_FetchXml = $"<condition attribute='sfa_name' operator='in'>{string.Join("", differentItems.Select(item => $"<value>{item}</value>"))}</condition>";
                        //将不同项的型番内容转成查询PSI详细表的xml
                        string PSI_Details_DiffModels_FetchXml = $"<condition attribute='sfa_p_names' operator='in'>{string.Join("", differentItems.Select(item => $"<value>{item}</value>"))}</condition>";
                        //从导入管理中获取不同型番对应的法人组合
                        EntityCollection PSI_DiffModelsAndLegal = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI("B001", yearXml, sfa_fetch_legalName, PSI_Diff_Models_FetchXml), serviceClient);
                        //存储PSI型番和型番对应的法人
                        var psi_Diff_ModelsAndlegal = PSI_DiffModelsAndLegal.Entities
            .Where(entity => entity.Contains("sfa_kunnr") && entity.Contains("sfa_zcusmodel"))
            .Select(entity => new
            {
                sfa_kunnr = ((EntityReference)entity["sfa_kunnr"]).Name,  // 获取 sfa_kunnr 的 Name
                sfa_kunnr_code = entity.GetAttributeValue<AliasedValue>("EMP1.sfa_sapcode").Value,
                sfa_zcusmodel = ((EntityReference)entity["sfa_zcusmodel"]).Name,  // 获取 sfa_zcusmodel 的 Name
                sfa_zcusmodel_code = entity.GetAttributeValue<AliasedValue>("EMP2.sfa_sapcode").Value  // 获取 sfa_zcusmodel 的 Name

            })
            .ToList();
                        #region 调整数量

                        //根据登录番号，型番，月份从PSI詳細表中查询
                        EntityCollection psi_adjust_num = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_get_adjust_number", PSI_Details_DiffModels_FetchXml, psi_Month_fetch, psi_applicationNumber_fetchxml), serviceClient);
                        //存储12个月对应型番的调整数量的List
                        List<ModelMonthNum> all_month_AdjustNum = new List<ModelMonthNum>();
                        foreach (var model in psi_allModelsList)
                        {
                            for (int adjust_month = 1; adjust_month <= 12; adjust_month++)
                            {
                                bool flag = true;
                                foreach (var adjust_record in psi_adjust_num.Entities)
                                {
                                    //比较记录中的型番，月份，以及是否存在sfa_adjusted_quantities属性
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

                        #region 今回合計,コメント

                        //存储每个型番12个月的数量
                        List<ModelMonthNum> all_monthNum = new List<ModelMonthNum>();
                        //存储每个型番12个月的コメント
                        List<ModelMonthNum> all_ModelReasonList = new List<ModelMonthNum>();
                        //遍历不同型番的型番和法人的全部组合
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将今回数量相加
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

                        #region 前回合計
                        //根据versionNumber和PSI法人获取上一回目十二个月份传入型番的记录
                        EntityCollection psi_last_ModelsNum = serviceClient.RetrieveMultiple(new FetchExpression(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsNumber", psi_allModelsCode_fetch_RollingDeatil, psi_Month_fetch, sfa_fetch_psi_legalName, lastVersionNumberXml)));
                        //存储上一回目每个型番12个月的数量
                        List<ModelMonthNum> all_last_monthNum = new List<ModelMonthNum>();
                        //遍历前端传入型番和法人的全部组合
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 予算
                        //获取予算最新版本
                        EntityCollection result_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_B("Config_VersionControl_A001", "", "", ""), serviceClient);
                        //存储每个型番12个月的予算数量
                        List<ModelMonthNum> all_budget_monthNum = new List<ModelMonthNum>();
                        //最新-版本号
                        string versionnumber = "";
                        if (result_BVersion != null && result_BVersion?.Entities?.Count > 0)
                        {
                            var record_BVersion = result_BVersion?.Entities?.FirstOrDefault();
                            //最新-版本号
                            versionnumber = record_BVersion?["sfa_versionguid"].ToString();

                        }
                        else
                        {
                            return new BadRequestObjectResult(new VersionControlResponse
                            {
                                Status = StatusCodes.Status500InternalServerError.ToString(),
                                Message = "選択した法人には有効型番が存在していません、導入管理を確認してください"
                            });
                        }

                        //将PSI为true的法人作为查询予算明细的存储条件转换为FetchXML
                        string sfa_fetch_BudgetLegalName = $"<condition attribute='sfa_legalname' operator='in'>{string.Join("", sfa_legal_nameList.Select(item => $"<value>{item}</value>"))}</condition>";
                        //查寻当前版本num的xml
                        string versionBudgetXml = $"<condition attribute='sfa_versionguid' operator='eq' value='{versionnumber}'/>";
                        //获取予算详细
                        EntityCollection result_Budget_BVersion = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAllModelsBudget", psi_Month_fetch, psi_allModelsCode_fetch_BudgetDeatil, sfa_fetch_BudgetLegalName, versionBudgetXml), serviceClient);

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (result_Budget_BVersion.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = result_Budget_BVersion.Entities
                    .Where(entity =>
                        entity.Contains("sfa_modelname") &&
                        entity["sfa_modelname"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_legalname") &&
                        entity["sfa_legalname"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将予算数量相加
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

                        #region 直送前半
                        //存储当前回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_direct_beforeNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 直送后半
                        //存储当前回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_direct_afterNum = new List<ModelMonthNum>();
                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_ModelsAndLegal.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_ModelsAndLegal.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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

                        #region 前回直送前半
                        //存储上一回目每个型番12个月直送前半的数量
                        List<ModelMonthNum> all_last_direct_before_monthNum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将直送前半相加
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

                        #region 前回直送后半
                        //存储上一回目每个型番12个月直送后半的数量
                        List<ModelMonthNum> all_last_direct_after_monthNum = new List<ModelMonthNum>();
                        //按照型番分组

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;
                                //比较滚动予算表中存在的记录
                                if (psi_last_ModelsNum.Entities.Count != 0)
                                {
                                    //获取有效的型番和法人组合
                                    var validCombination = psi_last_ModelsNum.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_name") &&
                        entity["sfa_p_name"].ToString() == model_legal.sfa_zcusmodel &&  // 匹配 sfa_zcusmodel
                        entity.Contains("sfa_c_name") &&
                        entity["sfa_c_name"].ToString() == model_legal.sfa_kunnr) // 匹配 sfa_kunnr
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将直送后半相加
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

                        #region AI予測
                        EntityCollection psi_AINumber = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_GetAINumber", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //存储每个型番12个月的AI予测的数量
                        List<ModelMonthNum> all_monthAINum = new List<ModelMonthNum>();

                        foreach (var model_legal in psi_Diff_ModelsAndlegal)
                        {
                            //m代表12个月份
                            for (int m = 1; m <= 12; m++)
                            {
                                bool flag = false;

                                if (psi_AINumber.Entities.Count != 0)
                                {
                                    //获取有效的型番code和法人code组合
                                    var validCombination = psi_AINumber.Entities
                    .Where(entity =>
                        entity.Contains("sfa_p_sapcode") &&
                        entity["sfa_p_sapcode"].ToString() == model_legal.sfa_zcusmodel_code.ToString() &&  // 匹配 sfa_zcusmodel_code
                        entity.Contains("sfa_c_sapcode") &&
                        entity["sfa_c_sapcode"].ToString() == model_legal.sfa_kunnr_code.ToString()) // 匹配 sfa_kunnr_code
                    .ToList();
                                    //如果有符合进行对应记录的添加
                                    if (validCombination.Any())
                                    {
                                        // 遍历有效的组合，进行后续操作
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
                                        //如果flag为false说明没有对应的月份值或者没有对应的型番和法人组合
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
                        //根据型番和月份将AI数量相加
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

                        #region 実績,実売,粗利率
                        //定义存放粗利结果的列表
                        List<ModelMonthNum> grossProfit_num = new List<ModelMonthNum>();
                        // 定义一个存放実売结果的列表
                        List<ModelMonthNum> achi_num = new List<ModelMonthNum>();
                        // 定义一个存放実績结果的列表
                        List<ModelMonthNum> achi_des_num = new List<ModelMonthNum>();
                        //根据年度，月份，型番的SAPcode和法人的SAPcode拿到売上実績テーブル表中对应的记录
                        EntityCollection psi_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);
                        //根据年度，月份，型番的SAPcode和法人的SAPcode拿到受损表中对应的记录
                        EntityCollection psi_Des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", sfa_fetch_p_codeXml, sfa_fetch_c_codeXml, psi_Month_fetch, yearXml), serviceClient);

                        // 按月份和sap_p_code进行分组
                        var achi_monthlySums = psi_achi_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                    GrossProfitSum = group.Sum(record => (decimal)record["sfa_grossprofit"]) // 计算sfa_grossprofit总和
                                }
                            );
                        // 按月份和sap_p_code进行分组
                        var des_monthlySums = psi_Des_number.Entities
                            .Where(record => record.Contains("sfa_month") && record.Contains("sfa_quantity") && record.Contains("sfa_p_sapcode"))
                            .GroupBy(record => new { Month = (int)record["sfa_month"], SapCode = (string)record["sfa_p_sapcode"] }) // 按月份和sap_p_code分组
                            .ToDictionary(
                                group => (group.Key.Month, group.Key.SapCode), // 使用元组作为字典的键
                                group => new
                                {
                                    QuantitySum = group.Sum(record => (decimal)record["sfa_quantity"]), // 计算sfa_quantity总和
                                    GrossProfitSum = group.Sum(record => record.Contains("sfa_grossprofit") && record["sfa_grossprofit"] != null ? (decimal)record["sfa_grossprofit"] : 0) // 计算sfa_grossprofit总和
                                }
                            );



                        // 将型番codeList转换为HashSetList
                        HashSet<string> sfa_p_code_hashSet = new HashSet<string>(sfa_p_code_list);

                        foreach (var scode in sfa_p_code_hashSet)
                        {
                            // 计算実績，毛利，実売的值
                            for (int m = 1; m <= 12; m++)
                            {
                                // 获取achi和des中对应月份的数量
                                string p_code = "";
                                decimal achiQuantity = 0;
                                decimal desQuantity = 0;
                                decimal achiGrossProfit = 0, desGrossProfit = 0;
                                p_code = scode;
                                // 如果achi_monthlySums中存在该月份的数据，则获取数量和毛利
                                if (achi_monthlySums.ContainsKey((m, scode)))
                                {
                                    achiQuantity = achi_monthlySums[(m, scode)].QuantitySum;
                                    achiGrossProfit = achi_monthlySums[(m, scode)].GrossProfitSum;
                                }

                                // 如果des_monthlySums中存在该月份的数据，则获取数量
                                if (des_monthlySums.ContainsKey((m, scode)))
                                {
                                    desQuantity = des_monthlySums[(m, scode)].QuantitySum;
                                    desGrossProfit = des_monthlySums[(m, scode)].GrossProfitSum;
                                }
                                // 将数量相加
                                decimal totalQuantity = achiQuantity + desQuantity;
                                //将毛利相加
                                decimal totalGrossProfit = achiGrossProfit + desGrossProfit;
                                //実売值
                                decimal totalAchiQuantity = achiQuantity;
                                // 创建一个ModelMonthNum对象并将其加入结果列表
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

                        #region 同期

                        //遍历获取到型番的KeyCode集合
                        HashSet<string> models_kecode_list = new HashSet<string>();
                        List<ModelMonthNum> models_kecode = new List<ModelMonthNum>();
                        foreach (var kecode_record in psi_allModels_keycode.Entities)
                        {
                            if (kecode_record.Contains("sfa_keycode") && kecode_record["sfa_keycode"] != null)
                            {
                                models_kecode_list.Add(kecode_record["sfa_keycode"].ToString());
                            }
                        }

                        //根据获取到的KeyCode拼接为XML，根据KeyCodeXML获取商品表中对应的型番的Sapcode
                        string model_kecode_fetchxml = $"<condition attribute='sfa_keycode' operator='in'>{string.Join("", models_kecode_list.Select(item => $"<value>{item}</value>"))}</condition>";
                        //根据KeycdoeXML获取商品表中对应的型番的Sapcode
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

                        //从 psi_keycode_allModels中提取型番的sfa_sapcode并拼接成查询实绩和受损表中的XML 条件
                        string allModel_kecode_fetchxml = $"<condition attribute='sfa_p_sapcode' operator='in'>{string.Join("", psi_keycode_allLegal.Entities
        .Where(entity => entity.Attributes.ContainsKey("sfa_sapcode")) // 检查是否包含 sfa_sapcode 属性
        .Select(entity => $"<value>{entity["sfa_sapcode"]}</value>"))}</condition>";

                        //根据allModel_kecode_fetchxml和sfa_fetch_psi_legalcode_xml到实绩表和受损表中查询去年对应的数量
                        EntityCollection psi_keycode_all_achi_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Achi_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        EntityCollection psi_keycode_all_des_number = CDSHelper.RetrieveAllRecords(FetchXMLHelper.GetFetchXML_BR_PSI_Deatail("PSI_Get_Des_number", allModel_kecode_fetchxml, sfa_fetch_psi_legalcode_xml, psi_Month_fetch, lastYearXml), serviceClient);
                        // 根据SapCode和月份分组并计算实绩和受损数量总和
                        var achiGroupedByKeycodeAndMonth = psi_keycode_all_achi_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // 按 Keycode 和月份分组
                            .ToDictionary(
                                group => group.Key, // 分组的键为 Keycode 和 Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                            );
                        var desGroupedByKeycodeAndMonth = psi_keycode_all_des_number.Entities
                            .Where(entity => entity.Attributes.ContainsKey("sfa_p_sapcode") && entity.Attributes.ContainsKey("sfa_month") && entity.Attributes.ContainsKey("sfa_quantity")) // 确保字段存在
                            .GroupBy(entity => new
                            {
                                sapcode = (string)entity["sfa_p_sapcode"],
                                Month = (int)entity["sfa_month"]
                            }) // 按 Keycode 和月份分组
                            .ToDictionary(
                                group => group.Key, // 分组的键为 Keycode 和 Month
                                group => group.Sum(entity => (decimal)entity["sfa_quantity"]) // 计算每组的 sfa_quantity 总和
                            );

                        //将achiGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model 对应 sapcode
                                Month = model.Month
                            };

                            if (achiGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum = value; // 赋值对应的总和
                            }
                        }

                        //将desGroupedByKeycodeAndMonth和models_kecode中根据sapcode和month对应的ModelSum的值进行赋值
                        foreach (var model in models_kecode)
                        {
                            var key = new
                            {
                                sapcode = model.Model, // Model 对应 sapcode
                                Month = model.Month
                            };

                            if (desGroupedByKeycodeAndMonth.TryGetValue(key, out decimal value))
                            {
                                model.ModelSum += value; // 将值相加
                            }
                        }
                        var mergedModelsKecode = models_kecode
            .GroupBy(item => new { item.Keycode, item.Month }) // 按 Keycode 和 Month 分组
            .Select(group => new ModelMonthNum
            {
                Keycode = group.Key.Keycode,
                Month = group.Key.Month,
                Model = string.Join(";", group.Select(x => x.Model)), // 合并 Model 值（如需要）
                ModelSum = group.Sum(x => x.ModelSum) // 汇总 ModelSum
            })
            .ToList();

                        #endregion

                        //存入大宽表中
                        foreach (var item in round_Number)
                        {
                            // 创建一行数据
                            DataRow newRow = dt.NewRow();
                            #region 今回合计
                            // 为 sfa_month 列赋值，月份
                            newRow["sfa_month"] = item.Month; // 赋值月份

                            // 为 sfa_p_names 列赋值，Model
                            newRow["sfa_p_names"] = item.Model; // 赋值 Model

                            // 为 sfa_all_sum_number 列赋值，ModelSum（来自 round_Number）
                            newRow["sfa_all_sum_number"] = item.ModelSum; // 赋值 round_Number 的 ModelSum
                            #endregion
                            //根据型番的item.Model的值与psi_ModelsAndLegal的sfa_p_code的值进行匹配
                            foreach (var pname in psi_ModelsAndLegal.Entities)
                            {
                                if (pname.Contains("sfa_p_name") && pname["sfa_p_name"].ToString() == item.Model && pname.Contains("sfa_c_name"))
                                {
                                    newRow["sfa_p_sapcode"] = pname.Contains("sfa_p_sapcode") ? pname["sfa_p_sapcode"].ToString() : ""; // 赋值 direct_beforeNumber 的 ModelSum
                                    newRow["sfa_c_names"] = pname["sfa_c_name"].ToString(); // 赋值 direct_beforeNumber 的 ModelSum
                                    break;
                                }

                            }
                            #region 前回合计
                            // 查找对应的 lastRound_Number 数据
                            var lastRound = lastRound_Number
                                            .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (lastRound != null)
                            {
                                newRow["sfa_allsumnumber_pre"] = lastRound.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_allsumnumber_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回差
                            // 计算前回差（今回合計 - 前回合計）
                            var previousDiff = item.ModelSum - (lastRound != null ? lastRound.ModelSum : 0);
                            // 为 sfa_num_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_num_balance"] = previousDiff;
                            #endregion
                            #region 予算
                            // 查找对应的预算数据
                            var budgetDetail = eachModel_BugetNumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的预算数据，则为预算列赋值
                            if (budgetDetail != null)
                            {
                                newRow["sfa_budget_num"] = budgetDetail.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的预算数据，设置为0或者其他默认值
                                newRow["sfa_budget_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 同期
                            // 查找对应的同期数据
                            var samePeriod_num = mergedModelsKecode
                                                .FirstOrDefault(x => (x.Model).Contains(newRow["sfa_p_code"].ToString()) && x.Month == item.Month);

                            // 如果找到对应的预算数据，则为预算列赋值
                            if (samePeriod_num != null)
                            {
                                newRow["sfa_sameperiod_num"] = samePeriod_num.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的预算数据，设置为0或者其他默认值
                                newRow["sfa_sameperiod_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送前半
                            // 查找对应的直送前半数据
                            var direct_beforeNumber = round_direct_beforeNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before"] = direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_direct_before"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送后半
                            // 查找对应的直送后半数据
                            var direct_afterNumber = round_direct_afterNum
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送后半数据，则为直送后半列赋值
                            if (direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after"] = direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                                newRow["sfa_direct_after"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回直送前半
                            // 查找对应的直送前半数据
                            var lastRound_direct_beforeNumber = lastRound_direct_before_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (lastRound_direct_beforeNumber != null)
                            {
                                newRow["sfa_direct_before_pre"] = lastRound_direct_beforeNumber.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_direct_before_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 前回直送後半
                            // 查找对应的直送后半数据
                            var last_direct_afterNumber = lastRound_direct_after_Number
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送后半数据，则为直送后半列赋值
                            if (last_direct_afterNumber != null)
                            {
                                newRow["sfa_direct_after_pre"] = last_direct_afterNumber.ModelSum; // 赋值 direct_afterNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送后半数据，设置为0或者其他默认值
                                newRow["sfa_direct_after_pre"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 直送前半差
                            //计算直送前半差(直送前半-前回直送前半)
                            var before_half_dif = direct_beforeNumber.ModelSum - lastRound_direct_beforeNumber.ModelSum;
                            // 为 sfa_direct_before_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_direct_before_balance"] = before_half_dif;
                            #endregion
                            #region 直送後半差
                            //计算直送後半差(直送後半-前回直送後半)
                            var after_half_dif = direct_afterNumber.ModelSum - last_direct_afterNumber.ModelSum;
                            // 为 sfa_direct_after_balance 列赋值，前回差（今回合計 - 前回合計）
                            newRow["sfa_direct_after_balance"] = after_half_dif;
                            #endregion
                            #region AI予測
                            // 查找对应的AI予测数据
                            var AI_number = round_AINumber
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (AI_number != null)
                            {
                                newRow["sfa_ai_num"] = AI_number.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_ai_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 実績
                            var achi_des_record = achi_des_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (achi_des_record != null)
                            {
                                newRow["sfa_sales_perf_num"] = achi_des_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_perf_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 実売
                            var achi_record = achi_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (achi_record != null)
                            {
                                newRow["sfa_sales_retail_num"] = achi_record.ModelSum; // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_sales_retail_num"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 粗利率
                            var gro_record = grossProfit_num
                                            .FirstOrDefault(x => x.Model == newRow["sfa_p_sapcode"].ToString() && x.Month == item.Month);

                            // 如果找到了对应的 lastRound 数据，则为 sfa_allsumnumber_pre 列赋值
                            if (gro_record != null && achi_des_record.ModelSum != 0)
                            {
                                newRow["sfa_perf_grossprofit"] = Math.Round(gro_record.ModelSum / achi_des_record.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_perf_grossprofit"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 進捗率
                            //実績/前回合計
                            if (achi_des_record != null && lastRound.ModelSum != 0)
                            {
                                newRow["sfa_progressrate"] = Math.Round(achi_des_record.ModelSum / lastRound.ModelSum, 3); // 赋值 lastRound_Number 的 ModelSum
                            }
                            else
                            {
                                newRow["sfa_progressrate"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region コメント
                            var model_reason = all_ModelReasonList
                                                .FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);

                            // 如果找到对应的直送前半数据，则为直送前半列赋值
                            if (model_reason != null)
                            {
                                newRow["sfa_num_reason"] = model_reason.Reason; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的评论数据，设置为0或者其他默认值
                                newRow["sfa_num_reason"] = ""; // 如果没有找到对应数据，则赋值为 "" 或者其他默认值
                            }
                            #endregion
                            #region 调整数量
                            var Adjust_num = all_month_AdjustNum.FirstOrDefault(x => x.Model == item.Model && x.Month == item.Month);
                            if (Adjust_num != null)
                            {
                                newRow["sfa_adjusted_quantities"] = Adjust_num.ModelSum; // 赋值 direct_beforeNumber 的 ModelSum
                            }
                            else
                            {
                                // 如果没有找到对应的直送前半数据，设置为0或者其他默认值
                                newRow["sfa_adjusted_quantities"] = 0; // 如果没有找到对应数据，则赋值为 0 或者其他默认值
                            }
                            #endregion
                            #region 出荷比率

                            #endregion
                            // 将新行添加到 DataTable
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
            //返回前端特定格式
            public List<object> FormatResult(DataTable dt, string applicationNumber)
            {
                var results = new List<object>();
                var models = dt.AsEnumerable().Select(row => row.Field<string>("sfa_p_names")).Distinct();
                var legals = dt.AsEnumerable().Select(row => row.Field<string>("sfa_c_names")).Distinct();

                foreach (var legal in legals)
                {
                    foreach (var model in models)
                    {
                        // 检查该法人与型番是否有效组合
                        if (dt.AsEnumerable().Any(row => row.Field<string>("sfa_c_names") == legal && row.Field<string>("sfa_p_names") == model))
                        {
                            results.Add(Create_AdjustNum_Entry(dt, legal, model, applicationNumber, "調整数量", "sfa_adjusted_quantities"));
                            results.Add(Create_Num_Entry(dt, legal, model, "今回合計", "sfa_all_sum_number"));
                            results.Add(Create_Num_PreEntry(dt, legal, model, "前回合計", "sfa_allsumnumber_pre"));
                            results.Add(Create_Num_BalanceEntry(dt, legal, model, applicationNumber, "前回差", "sfa_num_balance"));
                            results.Add(Create_Num_ReasonEntry(dt, legal, model, applicationNumber, "コメント", "sfa_num_reason"));
                            results.Add(Create_Budget_NumEntry(dt, legal, model, applicationNumber, "予算", "sfa_budget_num"));
                            results.Add(Create_SamePeriod_NumEntry(dt, legal, model, applicationNumber, "同期", "sfa_sameperiod_num"));
                            results.Add(Create_PerfNumEntry(dt, legal, model, applicationNumber, "実績数量", "sfa_sales_perf_num"));
                            results.Add(Create_Retail_NumEntry(dt, legal, model, applicationNumber, "実売数量", "sfa_sales_retail_num"));
                            results.Add(Create_ProgressRate(dt, legal, model, applicationNumber, "進捗率", "sfa_progressrate"));
                            results.Add(Create_Perf_GrossprofitEntry(dt, legal, model, applicationNumber, "粗利率", "sfa_perf_grossprofit"));
                            results.Add(Create_BeforeEntry(dt, legal, model, applicationNumber, "直送前半", "sfa_direct_before"));
                            results.Add(Create_AfterEntry(dt, legal, model, applicationNumber, "直送後半", "sfa_direct_after"));
                            results.Add(Create_Before_PreEntry(dt, legal, model, "前回直送前半", "sfa_direct_before_pre"));
                            results.Add(Create_Before_BalEntry(dt, legal, model, "直送前半差", "sfa_direct_before_balance"));
                            results.Add(Create_After_PreEntry(dt, legal, model, "前回直送後半", "sfa_direct_after_pre"));
                            results.Add(Create_After_BalEntry(dt, legal, model, "直送後半差", "sfa_direct_after_balance"));
                            results.Add(Create_AINum_Entry(dt, legal, model, "AI予測", "sfa_ai_num"));
                            results.Add(Create_SRNum_Entry(dt, legal, model, "出荷比率", ""));

                        }
                    }
                }
                return results; // 返回结果集
            }
            //調整数量
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
            //今回合计
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
            //進捗率
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
            //粗利率
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
            //コメント
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
            //前回数
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
            //前回差
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
            //预算
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
            //同期
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
            //実績数量
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
            //実売数量
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
            //直送前半
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
            //直送後半
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
            //前回直送前半
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
            //前回直送後半
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
            //直送前半差
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
            //直送後半差
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
            //出荷比率
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
        // 创建 DataTable 方法
        public static DataTable CreateDataTableFromDataverseSchema(string entityLogicalName, ServiceClient serviceClient)
        {
            DataTable dt = new DataTable(); // 创建 DataTable

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
                            dt.Columns.Add(columnName, typeof(Guid)); // 查找字段 ID
                            dt.Columns.Add(columnName + "_Formatted", typeof(string)); // 查找字段名称
                        }
                        else if (attribute.AttributeTypeName.Value == "PicklistType")
                        {
                            dt.Columns.Add(columnName, typeof(int)); // 选项值
                            dt.Columns.Add(columnName + "_Formatted", typeof(string)); // 选项文本
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
                _ => typeof(string) // 默认类型
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
