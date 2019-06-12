import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			List<String> ret = new ArrayList<>();
			int size = password.size();
			for (String onePwd : password){
				String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
				ret.add(oneHash);
			}
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			List<Boolean> ret = new ArrayList<>();
			int size = password.size();
			for (int i = 0; i < size; i++){
				String onePwd = password.get(i);
				String oneHash = hash.get(i);
				ret.add(BCrypt.checkpw(onePwd, oneHash));
			}
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
    }

	public boolean registerBE(String hostName, short port) throws IllegalArgument, org.apache.thrift.TException{
		try{
			return true;
		} catch (Exception e){
			throw new IllegalArgument(e.getMessage());
		}
	}

	public boolean deregisterBE(String hostName, short port) throws IllegalArgument, org.apache.thrift.TException{
		try{
			return true;
		} catch (Exception e){
			throw new IllegalArgument(e.getMessage());
		}
	}
}
