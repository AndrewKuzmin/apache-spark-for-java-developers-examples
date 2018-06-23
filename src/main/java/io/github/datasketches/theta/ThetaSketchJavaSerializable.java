package io.github.datasketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by Andrew on 6/23/2018.
 *
 * Wrapper to make ThetaSketch serializable:
 *
 */
class ThetaSketchJavaSerializable implements Serializable {

    private Sketch sketch;

    public ThetaSketchJavaSerializable() {
    }

    public ThetaSketchJavaSerializable(final Sketch sketch) {
        this.sketch = sketch;
    }

    public Sketch getSketch() {
        return sketch;
    }

    public CompactSketch getCompactSketch() {
        if (sketch == null) return null;
        if (sketch instanceof UpdateSketch) return ((UpdateSketch) sketch).compact();
        return (CompactSketch) sketch;
    }

    public void update(final String value) {
        if (sketch == null) sketch = UpdateSketch.builder().build();
        if (sketch instanceof UpdateSketch) {
            ((UpdateSketch) sketch).update(value);
        } else {
            throw new RuntimeException("update() on read-only sketch");
        }
    }

    public double getEstimate() {
        if (sketch == null) return 0.0;
        return sketch.getEstimate();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        if (sketch == null) {
            out.writeInt(0);
            return;
        }
        final byte[] serializedSketchBytes = ((UpdateSketch) sketch).compact().toByteArray();
        out.writeInt(serializedSketchBytes.length);
        out.write(serializedSketchBytes);
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        final int length = in.readInt();
        if (length == 0) return;
        final byte[] serializedSketchBytes = new byte[length];
        in.readFully(serializedSketchBytes);
        sketch = Sketches.wrapSketch(Memory.wrap(serializedSketchBytes));
    }

}
