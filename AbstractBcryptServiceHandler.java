import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

abstract class AbstractBcryptServiceHandler implements BcryptService.Iface {
    public List<String> hashPassword(
            List<String> passwordList,
            short logRounds
    ) throws IllegalArgument, org.apache.thrift.TException {
		try {
			List<String> ret = new ArrayList<>();
			for (String password: passwordList) {
                ret.add(BCrypt.hashpw(password, BCrypt.gensalt(logRounds)));
            }
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> checkPassword(
            List<String> passwordList,
            List<String> hashList
    ) throws IllegalArgument, org.apache.thrift.TException  {
        try {
            List<Boolean> ret = new ArrayList<>();
            for(int i = 0; i < passwordList.size(); i++) {
                boolean match;
                try {
                    match = BCrypt.checkpw(passwordList.get(i), hashList.get(i));
                } catch (Exception e) {
                    match  = false;
                }
                ret.add(match);
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }
}
