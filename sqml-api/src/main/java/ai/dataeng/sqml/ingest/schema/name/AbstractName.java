package ai.dataeng.sqml.ingest.schema.name;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractName implements Name {

    public static String validateName(String name) {
        Preconditions.checkArgument(!StringUtils.isEmpty(name),"Not a valid name: %s",name);
        return name;
    }

    @Override
    public String toString() {
        return getDisplay();
    }

    @Override
    public boolean equals(Object other) {
        if (other==null) return false;
        else if (this==other) return true;
        else if (!(other instanceof Name)) return false;
        Name o = (Name) other;
        return getCanonical().equals(o.getCanonical());
    }

    @Override
    public int hashCode() {
        return getCanonical().hashCode();
    }

    @Override
    public int compareTo(Name o) {
        return getCanonical().compareTo(o.getCanonical());
    }

}
