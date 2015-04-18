package work.KPI;

import java.io.Serializable;
import java.util.Scanner;
/**
 * 
 * @author zhanghua
 * http://www.dataguru.cn/forum.php?mod=viewthread&tid=249766&page=1&extra=#pid825329
 *
 */
public class NginxLog implements Serializable {
        private static final long serialVersionUID = 1L;
        private String ip;//��¼�ͻ��˵�ip��ַ, 222.68.172.190
        private String method;
        private String url;//��¼�����url��httpЭ��, ��GET /images/my.jpg HTTP/1.1��
        private String status;//��¼����״̬,�ɹ���200, 200
        private int pageSize;//��¼���͸�ͻ����ļ��������ݴ�С, 19939
        private String httpReferer;//������¼���Ǹ�ҳ�����ӷ��ʹ�����, ��http://www.angularjs.cn/A00n��
        private String agent;//��¼�ͻ�������������Ϣ,
        private boolean valid = true;// �ж�����Ƿ�Ϸ�
        
        // 111.13.8.250 - - [05/Jan/2012:00:00:04 +0800] "GET http:///thread-476025-1-1.html HTTP/1.0" 200 99628 "-" "Mozilla/4.0"
        public static NginxLog parse(String line) {
                if (line == null || line.length() == 0) {
                        return null;
                }
                int index = 0;
                // ip
                int endPos = line.indexOf("-");
                String ip = substring(line, index, endPos);
                index = endPos;
                //method
                index = line.indexOf("] ", index);
                endPos = line.indexOf(" ", index+2);
                String method = substring(line, index+3, endPos);
                index = endPos;
                // url
                endPos = line.indexOf("HTTP/", index);
                String url = substring(line, index, endPos);
                if (url!=null&&url.contains("?")) {
                        url = url.substring(0, url.indexOf("?"));
                }
                index = endPos;
                // status
                index = line.indexOf(" ", index);
                endPos = line.indexOf(" ", index + 1);
                String status = substring(line, index, endPos);
                index = endPos;
                // pageSize
                endPos = line.indexOf(" ", index + 1);
                String pageSize = substring(line, index, endPos);
                index = endPos;
                // httpReferer
                endPos = line.indexOf(" ", index + 1);
                String httpReferer = substring(line, index, endPos);
                index = endPos;
                //agent
                index=endPos = line.indexOf("\"", index);
                endPos = line.lastIndexOf("\"");
                String agent = substring(line, index+1, endPos);
                //http referer ��agent���Ǳ����
//                if (StringUtils.IsEmpty(ip,method, url, status, pageSize)) {
//                        return null;
//                }                

                NginxLog nginxLine = new NginxLog();
                nginxLine.setIp(ip);
                nginxLine.setMethod(method);
                nginxLine.setUrl(url);
                nginxLine.setStatus(status);
                nginxLine.setPageSize(Integer.parseInt(pageSize));
                nginxLine.setHttpReferer(httpReferer);
                nginxLine.setAgent(agent);
                
                if (status == "400") {
   				 	nginxLine.setValid(false);
				}
                
                return nginxLine;
        }

        private static String substring(String line, int index, int endPos) {
                if(endPos<index){
                        return null;
                }
                if (index == -1) {
                        return null;
                }
                if (endPos == -1) {
                        return null;
                }
                String phase = null;
                try {
                        phase = line.substring(index, endPos).trim();
                } catch (Exception e) {
                        e.printStackTrace();
                }
                return phase;
        }
        
        private boolean isValid() {
        	return valid;
		}
        
        private void setValid(boolean bool){
        	valid = bool;
        }

        /**
         * @return the ip
         */
        public String getIp() {
                return ip;
        }
        
        /**
         * @return the method
         */
        public String getMethod() {
                return method;
        }

        /**
         * @param method the method to set
         */
        public void setMethod(String method) {
                this.method = method;
        }

        /**
         * @param ip
         *            the ip to set
         */
        public void setIp(String ip) {
                this.ip = ip;
        }

        /**
         * @return the url
         */
        public String getUrl() {
                return url;
        }

        /**
         * @param url
         *            the url to set
         */
        public void setUrl(String url) {
                this.url = url;
        }

        /**
         * @return the status
         */
        public String getStatus() {
                return status;
        }

        /**
         * @param status
         *            the status to set
         */
        public void setStatus(String status) {
                this.status = status;
        }

        /**
         * @return the pageSize
         */
        public int getPageSize() {
                return pageSize;
        }

        /**
         * @param pageSize
         *            the pageSize to set
         */
        public void setPageSize(int pageSize) {
                this.pageSize = pageSize;
        }

        /**
         * @return the httpReferer
         */
        public String getHttpReferer() {
                return httpReferer;
        }

        /**
         * @param httpReferer
         *            the httpReferer to set
         */
        public void setHttpReferer(String httpReferer) {
                this.httpReferer = httpReferer;
        }

        /**
         * @return the agent
         */
        public String getAgent() {
                return agent;
        }

        /**
         * @param agent the agent to set
         */
        public void setAgent(String agent) {
                this.agent = agent;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
                return "NginxLine [agent=" + agent + ", httpReferer=" + httpReferer + ", ip=" + ip + ", method=" + method
                                + ", pageSize=" + pageSize + ", status=" + status + ", url=" + url + "]";
        }

        public static void main(String[] args) {
                Scanner scan = new Scanner(System.in);
                while (true) {
                        String line = scan.nextLine();
                        System.out.println(parse(line));
                }
        }

}
