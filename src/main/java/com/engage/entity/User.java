
package com.engage.entity;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class User {
  private String vizId;
  private String param;
  private String pid;
  private String gcmId;
  private String accountId;
  private Long lastSent;
  private Integer sentCount;

  public User() {
  }

  public User(String analyzeData) {
    parseAnalyzeData(analyzeData);
  }

  public String getVizId() {
    return vizId;
  }

  public void setVizId(String vizId) {
    this.vizId = vizId;
  }

  public String getParam() {
    return param;
  }

  public void setParam(String param) {
    this.param = param;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public String getGcmId() {
    return gcmId;
  }

  public void setGcmId(String gcmId) {
    this.gcmId = gcmId;
  }

  public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public Long getLastSent() {
    return lastSent;
  }

  public void setLastSent(Long lastSent) {
    this.lastSent = lastSent;
  }

  public Integer getSentCount() {
    return sentCount;
  }

  public void setSentCount(Integer sentCount) {
    this.sentCount = sentCount;
  }

  private void parseAnalyzeData(String analyzeData) {
    StringTokenizer st = new StringTokenizer(analyzeData);
    String[] values = new String[100];
    int i = 0;
    while (st.hasMoreElements()) {
      values[i++] = (String) st.nextElement();
    }
    this.vizId = values[3];
    this.accountId = values[9];
    String url = values[5];
    String[] pairs = url.split("&");
    Map<String, String> query_pairs = new LinkedHashMap<String, String>();
    for (String pair : pairs) {
      if (StringUtils.isBlank(pair))
        continue;
      int idx = pair.indexOf("=");
      try {
        query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
            URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        System.out.println(e.toString());
      }
    }
    this.param = query_pairs.get("param");
    this.gcmId = query_pairs.get("gcmid");
    if (param.equals("e400") || param.equals("e500")) {
      for (int j = 1; j <= 3; j++) {
        String p = query_pairs.get("pid" + j);
        if (p != null) {
          if (this.pid == null) {
            this.pid = p;
          } else {
            this.pid += "," + p;
          }
        }
      }
    } else {
      this.pid = query_pairs.get("pid");
    }
  }

  public void mergeUserData(User user) {
    if (this.pid != null) {
      this.pid += "," + user.pid;
    } else {
      this.pid = user.pid;
    }
    if (this.gcmId != user.gcmId) {
      this.gcmId = user.gcmId;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("User [vizId=").append(vizId).append(", param=").append(param).append(", pid=")
        .append(pid).append(", gcmId=").append(gcmId).append(", accountId=").append(accountId)
        .append(", lastSent=").append(lastSent).append(", sentCount=").append(sentCount)
        .append("]");
    return builder.toString();
  }
}
